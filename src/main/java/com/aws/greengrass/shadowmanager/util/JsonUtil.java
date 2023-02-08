/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.util;

import com.aws.greengrass.shadowmanager.exception.InvalidRequestParametersException;
import com.aws.greengrass.shadowmanager.model.ErrorMessage;
import com.aws.greengrass.shadowmanager.model.ShadowDocument;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersionDetector;
import com.networknt.schema.ValidationMessage;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import software.amazon.awssdk.aws.greengrass.model.ConflictError;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;

import static com.aws.greengrass.shadowmanager.model.Constants.DEFAULT_DOCUMENT_STATE_DEPTH;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_CLIENT_TOKEN;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_STATE;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_STATE_DESIRED;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_STATE_REPORTED;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_VERSION;
import static com.aws.greengrass.shadowmanager.model.ErrorMessage.createInvalidPayloadJsonMessage;

public final class JsonUtil {
    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static JsonSchema updateRequestSchema;

    static {
        OBJECT_MAPPER.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }

    private JsonUtil() {

    }

    /**
     * Load the update schema so it can be used for validation.
     *
     * @throws IOException if an error occurs loading or parsing the JSON schema
     */
    public static void loadSchema() throws IOException {
        try (InputStream schema = JsonUtil.class.getResourceAsStream("/json/schema/update_payload_schema.json")) {
            JsonNode updateRequest = OBJECT_MAPPER.reader().readTree(schema);
            updateRequestSchema =
                    JsonSchemaFactory.getInstance(SpecVersionDetector.detect(updateRequest)).getSchema(updateRequest);
        }
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
        Set<ValidationMessage> errors = updateRequestSchema.validate(payload);
        if (errors.isEmpty()) {
            return;
        }

        StringJoiner message = new StringJoiner(". ");
        // library adds "$" as the root node so to clean up the error messages we remove it
        // e.g. $.state.reported becomes state.reported
        errors.stream().map(ValidationMessage::getMessage).map(m -> m.replace("$.", "")).forEach(message::add);

        throw new InvalidRequestParametersException(createInvalidPayloadJsonMessage(message.toString()));
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

    public static byte[] getPayloadBytes(JsonNode node) throws JsonProcessingException {
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
    private static void validatePayloadStateDepth(JsonNode stateJson) throws InvalidRequestParametersException {
        calculateDepth(stateJson.get(SHADOW_DOCUMENT_STATE_REPORTED), 1, DEFAULT_DOCUMENT_STATE_DEPTH);
        calculateDepth(stateJson.get(SHADOW_DOCUMENT_STATE_DESIRED), 1, DEFAULT_DOCUMENT_STATE_DEPTH);
    }

    private static int calculateDepth(final JsonNode node, final int currentDepth, final int maxDepth)
            throws InvalidRequestParametersException {
        if (node == null || node.isEmpty()) {
            return 0;
        }

        // If the current depth is greater than the max depth, then raise an invalid request parameters error for max
        //depth reached.
        if (currentDepth > maxDepth) {
            throw new InvalidRequestParametersException(ErrorMessage.INVALID_STATE_NODE_DEPTH_MESSAGE);
        }

        // Now recurse over all the children nodes if the node is not a value node to check their depth.
        int maxChildDepth = currentDepth;
        if (!node.isValueNode()) {
            for (JsonNode child : node) {
                int currentChildDepth = calculateDepth(child, currentDepth + 1, maxDepth);
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
     * @param sourceDocument  The current version of the shadow document.
     * @param updatedDocument The updated version of the shadow document sent in the request.
     * @throws ConflictError                     when the version number sent in the update request is not exactly one
     *                                           higher than the current shadow version
     * @throws InvalidRequestParametersException when the payload sent in the update request has bad data.
     * @throws IOException                       when the payload is not deserializable as JSON node.
     */
    public static void validatePayload(ShadowDocument sourceDocument, JsonNode updatedDocument)
            throws ConflictError, InvalidRequestParametersException, IOException {
        // Validate the state node of the payload for the depth.
        JsonNode stateJson = updatedDocument.get(SHADOW_DOCUMENT_STATE);
        JsonUtil.validatePayloadStateDepth(stateJson);

        JsonNode updateVersion = updatedDocument.get(SHADOW_DOCUMENT_VERSION);
        // If there is no version node in the update, then return since we don't need to check for conflicts.
        if (isNullOrMissing(updateVersion)) {
            return;
        }
        // If there is no current version document, then this is the first version of the document and we only need
        // to ensure that if there is a version in the update request, it is 1.
        if (sourceDocument.isNewDocument()) {
            if (updateVersion.asInt() != 1) {
                throw new InvalidRequestParametersException(ErrorMessage.INVALID_VERSION_MESSAGE);
            }
            return;
        }
        if (sourceDocument.getVersion() != updateVersion.asInt()) {
            throw new ConflictError(ErrorMessage.VERSION_CONFLICT_MESSAGE.getMessage());
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
     */
    public static Optional<String> getClientToken(final JsonNode payload) {
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
    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST", justification = "We do check the type before cast.")
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

    public static boolean hasVersion(JsonNode document) {
        return document.has(SHADOW_DOCUMENT_VERSION) && document.get(SHADOW_DOCUMENT_VERSION).isIntegralNumber();
    }

    public static long getVersion(JsonNode document) {
        return document.get(SHADOW_DOCUMENT_VERSION).asLong();
    }
}
