/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.util;

import com.aws.greengrass.shadowmanager.exception.InvalidRequestParametersException;
import com.aws.greengrass.shadowmanager.model.Constants;
import com.aws.greengrass.shadowmanager.model.ErrorMessage;
import com.aws.greengrass.shadowmanager.model.ShadowDocument;
import com.aws.greengrass.util.Utils;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.fge.jackson.JsonLoader;
import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import com.github.fge.jsonschema.core.report.ProcessingReport;
import com.github.fge.jsonschema.main.JsonSchema;
import com.github.fge.jsonschema.main.JsonSchemaFactory;
import software.amazon.awssdk.aws.greengrass.model.ConflictError;

import java.io.IOException;
import java.util.Iterator;
import java.util.Optional;
import java.util.StringJoiner;

import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_CLIENT_TOKEN;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_STATE;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_VERSION;
import static com.aws.greengrass.shadowmanager.model.Constants.STATE_NODE_REQUIRED_PARAM_ERROR_MESSAGE;

public final class JsonUtil {
    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static JsonSchema updateShadowRequestJsonSchema;
    private static JsonSchema updateShadowPayloadJsonSchema;

    static {
        OBJECT_MAPPER.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }

    private JsonUtil() {

    }

    /**
     * Sets up the shadow document request and state schema objects by reading from the resources file.
     *
     * @throws IOException         if the resource file does not exist.
     * @throws ProcessingException if the resource file has corrupted data which cannot be converted to JsonSchema.
     */
    public static void setUpdateShadowJsonSchema() throws IOException, ProcessingException {
        final JsonSchemaFactory factory = JsonSchemaFactory.byDefault();
        final JsonNode updateRequestJsonNode = JsonLoader.fromResource("/json/schema/update_payload_schema.json");
        updateShadowRequestJsonSchema = factory.getJsonSchema(updateRequestJsonNode);
        final JsonNode updatePayloadJsonNode = JsonLoader.fromResource("/json/schema/update_payload_state_schema.json");
        updateShadowPayloadJsonSchema = factory.getJsonSchema(updatePayloadJsonNode);

    }

    /**
     * Validate the JSON payload schema.
     *
     * @param payload the JSON payload to validate.
     * @throws InvalidRequestParametersException if there were any processing errors based on the specified
     *                                           schema
     */
    @SuppressWarnings("PMD.PreserveStackTrace")
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

    @SuppressWarnings("PMD.AvoidDeeplyNestedIfStmts")
    private static String getAllValidationErrors(ProcessingReport report) {
        StringJoiner errorMessages = new StringJoiner("\r\n");
        if (report != null) {

            report.forEach(processingMessage -> {
                // Make sure the error messages contain the field name.
                JsonNode instance = processingMessage.asJson().get("instance");
                if (!isNullOrMissing(instance)) {
                    JsonNode pointer = instance.get("pointer");
                    if (!isNullOrMissing(pointer) && !Utils.isEmpty(pointer.asText())) {
                        String fieldName = pointer.asText().substring(1);
                        processingMessage.setMessage(String.format("Invalid %s. %s", fieldName,
                                processingMessage.getMessage()));
                    }
                }
                errorMessages.add(processingMessage.getMessage());
            });
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
        return Optional.of(OBJECT_MAPPER.readTree(payload));
    }

    public static byte[] getPayloadBytes(JsonNode node) throws IOException {
        return OBJECT_MAPPER.writeValueAsBytes(node);
    }

    public static boolean isNullOrMissing(JsonNode node) {
        return node == null || node.isNull() || node.isMissingNode() || node.isObject() && node.isEmpty();
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
        int newDepth = node.isValueNode() ? currentDepth : currentDepth + 1;

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
    public static void validatePayload(ShadowDocument sourceDocument,
                                       byte[] updatedDocumentBytes)
            throws ConflictError, InvalidRequestParametersException, IOException {
        // If the payload size is greater than the maximum default shadow document size, then raise an invalid
        // parameters error for payload too large.
        //TODO: get the document size from AWS account.
        if (updatedDocumentBytes.length > Constants.DEFAULT_DOCUMENT_SIZE) {
            ErrorMessage errorMessage = ErrorMessage.createPayloadTooLargeMessage();
            throw new InvalidRequestParametersException(errorMessage);
        }
        Optional<JsonNode> updatedDocument = getPayloadJson(updatedDocumentBytes);
        if (!updatedDocument.isPresent() || isNullOrMissing(updatedDocument.get())) {
            ErrorMessage invalidPayloadJsonMessage = ErrorMessage.createInvalidPayloadJsonMessage("");
            throw new InvalidRequestParametersException(invalidPayloadJsonMessage);
        }
        // Validate the payload schema
        validatePayloadSchema(updatedDocument.get());

        // Validate the state node of the payload for the depth.
        JsonNode stateJson = updatedDocument.get().get(SHADOW_DOCUMENT_STATE);
        validatePayloadStateDepth(stateJson);

        // If there is no current version document, then this is the first version of the document and we only need
        // to en sure that if there is a version in the update request, it is 0.
        JsonNode updateVersion = updatedDocument.get().get(SHADOW_DOCUMENT_VERSION);
        if (sourceDocument.isNewDocument()) {
            if (!isNullOrMissing(updateVersion) && updateVersion.asInt() != 0) {
                throw new InvalidRequestParametersException(ErrorMessage.INVALID_VERSION_MESSAGE);
            }
            return;
        }
        if (sourceDocument.getVersion() + 1 != updateVersion.asInt()) {
            throw new ConflictError(ErrorMessage.INVALID_VERSION_MESSAGE.getMessage());
        }
    }

    /**
     * Checks the JSON node and returns a null if the node is empty.
     *
     * @param json The JSON node to check.
     * @return a null if the json node is null, empty or missing; Else returns the JSON node.
     */
    public static JsonNode nullIfEmpty(final JsonNode json) {
        if (isNullOrMissing(json)) {
            return null;
        }
        return json;
    }

    /**
     * Gets the client token from the JSON payload.
     *
     * @param payload the JSON payload.
     * @return an empty Optional if the token is not existent; Else returns the client token.
     * @throws InvalidRequestParametersException if the client token is null
     */
    public static Optional<String> getClientToken(final JsonNode payload) throws InvalidRequestParametersException {
        if (!payload.has(SHADOW_DOCUMENT_CLIENT_TOKEN)) {
            return Optional.empty();
        }

        final String token = payload.get(SHADOW_DOCUMENT_CLIENT_TOKEN).asText();
        return Optional.of(token);
    }

    /**
     * Calculate the delta between the reported and desired JSON.
     *
     * @param reported The reported JSON node.
     * @param desired  The desired JSON node.
     * @return the delta node containing the difference between the reported and desired.
     */
    public static JsonNode calculateDelta(JsonNode reported, JsonNode desired) {
        // If original and updated shadow documents are both objects then recursively diff
        if (reported.isObject() && desired.isObject()) {
            final ObjectNode result = calculateDelta((ObjectNode) reported, (ObjectNode) desired);
            return result.size() > 0 ? result : null;
        }

        // This handles the semantic number comparison.
        if (reported.isNumber() && desired.isNumber()) {
            return reported.asLong() == desired.asLong() && reported.asDouble() == desired.asDouble() ? null : desired;
        }

        // Now, if they both the original and updated shadow documents aren't objects or numbers, then they are either
        // both arrays, values, or differing types either way if they are different then they are completely different
        // and we can just return the updated node.
        if (!reported.equals(desired)) {
            return desired;
        }

        // At this point, both the original and updated are same and we return null.
        return null;
    }

    private static ObjectNode calculateDelta(final ObjectNode original, final ObjectNode updated) {
        final ObjectNode result = OBJECT_MAPPER.createObjectNode();
        // Iterate over the updated shadow document and compare the values to the original shadow document.
        final Iterator<String> fields = updated.fieldNames();

        while (fields.hasNext()) {
            final String field = fields.next();

            final JsonNode originalValue = original.get(field);
            final JsonNode updatedValue = updated.get(field);

            // If the updated node's value is null and original is not null, then that node is deleted
            // and we should set that node's value to null in the shadow document.
            if (isNullOrMissing(updatedValue)) {
                if (!isNullOrMissing(originalValue)) {
                    result.set(field, null);
                }
                continue;
            }

            // If the original value is null, then there is a new node to be added.
            if (isNullOrMissing(originalValue)) {
                result.set(field, updatedValue);
                continue;
            }

            // Otherwise, recurse on the node to see if there is any difference in the value of the
            // nodes.
            JsonNode diffResult = calculateDelta(originalValue, updatedValue);
            if (diffResult != null) {
                result.set(field, diffResult);
            }
        }
        return result;
    }
}
