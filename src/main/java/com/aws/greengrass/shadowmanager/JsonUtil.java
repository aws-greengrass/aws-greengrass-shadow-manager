/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager;

import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.shadowmanager.exception.InvalidRequestParametersException;
import com.aws.greengrass.shadowmanager.model.Constants;
import com.aws.greengrass.shadowmanager.model.ErrorMessage;
import com.aws.greengrass.shadowmanager.model.JsonShadowDocument;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.fge.jackson.JsonLoader;
import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import com.github.fge.jsonschema.core.report.ProcessingReport;
import com.github.fge.jsonschema.main.JsonSchema;
import com.github.fge.jsonschema.main.JsonSchemaFactory;
import software.amazon.awssdk.aws.greengrass.model.ConflictError;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Optional;
import java.util.StringJoiner;

import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_STATE;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_VERSION;
import static com.aws.greengrass.shadowmanager.model.Constants.STATE_NODE_REQUIRED_PARAM_ERROR_MESSAGE;

public final class JsonUtil {
    private static final Logger logger = LogManager.getLogger(JsonUtil.class);
    static ObjectReader objectReader;
    static ObjectWriter objectWriter;
    static ObjectMapper objectMapper;
    private static JsonSchema updateShadowRequestJsonSchema;
    private static JsonSchema updateShadowPayloadJsonSchema;

    static {
        try {
            final JsonSchemaFactory factory = JsonSchemaFactory.byDefault();
            final JsonNode updateRequestJsonNode = JsonLoader.fromResource("/json/schema/update_payload_schema.json");
            updateShadowRequestJsonSchema = factory.getJsonSchema(updateRequestJsonNode);
            final JsonNode updatePayloadJsonNode = JsonLoader
                    .fromResource("/json/schema/update_payload_state_schema.json");
            updateShadowPayloadJsonSchema = factory.getJsonSchema(updatePayloadJsonNode);

            objectMapper = new ObjectMapper();
            objectReader = objectMapper.reader();
            objectWriter = objectMapper.writer();
        } catch (ProcessingException | IOException e) {
            logger.atError().cause(e).log("Unable to parse JSON schema from resource files");
        }
    }

    private JsonUtil() {

    }

    /**
     * Validate the JSON payload schema.
     *
     * @param payload the JSON payload to validate.
     * @throws InvalidRequestParametersException if there were any processing errors based on the specified
     *                                           schema
     */
    public static void validatePayloadSchema(JsonNode payload) throws InvalidRequestParametersException {
        ProcessingReport report;
        try {
            report = updateShadowRequestJsonSchema.validate(payload);
            if (report.isSuccess()) {
                JsonNode state = payload.get(SHADOW_DOCUMENT_STATE);
                report = updateShadowPayloadJsonSchema.validate(state);
                if (report.isSuccess()) {
                    return;
                }
                throw new InvalidRequestParametersException(ErrorMessage
                        .createInvalidPayloadJsonMessage(STATE_NODE_REQUIRED_PARAM_ERROR_MESSAGE));
            }
            throw new InvalidRequestParametersException(ErrorMessage
                    .createInvalidPayloadJsonMessage(getAllValidationErrors(report)));
        } catch (ProcessingException e) {
            throw new InvalidRequestParametersException(ErrorMessage.createInternalServiceErrorMessage());
        }
    }

    private static String getAllValidationErrors(ProcessingReport report) {
        StringJoiner errorMessages = new StringJoiner("\r\n");
        if (report != null) {
            report.forEach(processingMessage -> errorMessages.add(processingMessage.getMessage()));
        }
        return errorMessages.toString();
    }

    /**
     * Gets the payload as a JSON node from a byte array.
     *
     * @param payload The byte array containing payload
     * @return the JSON payload
     * @throws IOException if the payload is not deserialized properly.
     */
    public static Optional<JsonNode> getPayloadJson(byte[] payload) throws IOException {
        if (payload == null) {
            return Optional.empty();
        }
        return Optional.of(objectReader.readTree(new ByteArrayInputStream(payload)));
    }

    public static byte[] getPayloadBytes(JsonNode node) throws IOException {
        return objectWriter.writeValueAsBytes(node);
    }

    public static boolean isNullOrMissing(JsonNode node) {
        return node == null || node.isNull() || node.isMissingNode();
    }

    /**
     * Creates an Object node using the static Object Mapper.
     *
     * @return an empty object node.
     */
    public static ObjectNode createObjectNode() {
        return objectMapper.createObjectNode();
    }

    /**
     * Validates that the state node depth is no deeper than the max depth for shadows (6).
     *
     * @param stateJson The state node to validate
     * @throws InvalidRequestParametersException when the state node has depth more than the max
     */
    private static int validatePayloadStateDepth(JsonNode stateJson) throws InvalidRequestParametersException {
        return calculateDepth(stateJson, 0, Constants.DEFAULT_DOCUMENT_STATE_DEPTH);
    }

    private static int calculateDepth(final JsonNode node, final int currentDepth, final int maxDepth)
            throws InvalidRequestParametersException {
        // If the Node is not a value node (i.e. either an object or an array), then add another level to the depth.
        int newDepth = (!node.isValueNode()) ? currentDepth + 1 : currentDepth;

        // If the new depth is greater than the max depth, then raise an invalid request parameters error for max
        //depth reached.
        if (newDepth > maxDepth) {
            throw new InvalidRequestParametersException(ErrorMessage.INVALID_STATE_NODE_DEPTH_MESSAGE);
        }

        // Now recurse over all the children nodes if the node is not a value node to check their depth.
        int maxChildDepth = newDepth;
        if (!node.isValueNode()) {
            for (JsonNode child : node) {
                int currentChildDepth = calculateDepth(child, newDepth, maxDepth);
                maxChildDepth = Math.max(currentChildDepth, maxChildDepth);
            }
        }
        return maxChildDepth;
    }

    /**
     * Validates the payload schema to ensure that the JSON has the correct schema, the state node schema to ensure
     * it's correctness, the depth of the state node to ensure it is within the boundaries and the version of the
     * payload.
     *
     * @param sourceDocument       The current version of the shadow document.
     * @param updatedDocumentBytes The updated version of the shadow document sent in the request.
     * @throws ConflictError                     when the version number sent in the update request is not exactly one
     *                                           higher than the current shadow version
     * @throws InvalidRequestParametersException when the payload sent in the update request has bad data.
     * @throws IOException                       when the payload is not deserizable as JSON node.
     */
    public static void validatePayload(JsonShadowDocument sourceDocument,
                                       byte[] updatedDocumentBytes)
            throws ConflictError, InvalidRequestParametersException, IOException {
        // If the payload size is greater than the maximum default shadow document size, then raise an invalid
        // parameters error for payload too large.
        //TODO: get the document size from AWS account.
        if (updatedDocumentBytes.length > Constants.DEFAULT_DOCUMENT_SIZE) {
            ErrorMessage errorMessage = ErrorMessage.createPayloadTooLargeMessage();
            throw new InvalidRequestParametersException(errorMessage);
        }
        Optional<JsonNode> updatedDocument = JsonUtil.getPayloadJson(updatedDocumentBytes);
        if (!updatedDocument.isPresent() || isNullOrMissing(updatedDocument.get())) {
            ErrorMessage invalidPayloadJsonMessage = ErrorMessage.createInvalidPayloadJsonMessage("");
            throw new InvalidRequestParametersException(invalidPayloadJsonMessage);
        }
        // Validate the payload schema
        JsonUtil.validatePayloadSchema(updatedDocument.get());

        // Validate the state node of the payload for the depth.
        JsonNode stateJson = updatedDocument.get().get(SHADOW_DOCUMENT_STATE);
        JsonUtil.validatePayloadStateDepth(stateJson);

        // If there is no current version document, then this is the first version of the document and we only need
        // to en sure that if there is a version in the update request, it is 0.
        JsonNode updateVersion = updatedDocument.get().get(SHADOW_DOCUMENT_VERSION);
        if (isNullOrMissing(sourceDocument.getDocument())) {
            if (!isNullOrMissing(updateVersion)  && updateVersion.asInt() != 0) {
                throw new InvalidRequestParametersException(ErrorMessage.INVALID_VERSION_MESSAGE);
            }
            return;
        }
        JsonNode sourceVersion = sourceDocument.getDocument().get(SHADOW_DOCUMENT_VERSION);
        if (sourceVersion.asInt() + 1 != updateVersion.asInt()) {
            throw new ConflictError(ErrorMessage.INVALID_VERSION_MESSAGE.getMessage());
        }
    }
}
