/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.model;

import com.aws.greengrass.shadowmanager.JsonUtil;
import com.aws.greengrass.shadowmanager.ipc.model.Operation;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import lombok.Getter;

import java.io.IOException;
import java.time.Instant;
import java.util.Iterator;
import java.util.Optional;

import static com.aws.greengrass.shadowmanager.JsonUtil.isNullOrMissing;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_CLIENT_TOKEN;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_STATE;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_STATE_CURRENT;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_STATE_DESIRED;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_STATE_PREVIOUS;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_STATE_REPORTED;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_TIMESTAMP;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_VERSION;

/**
 * Class to store the shadow document in JSON format and perform operations on it.
 */
//TODO: Handle metadata node
public class JsonShadowDocument {
    @Getter
    private final JsonNode document;
    @Getter
    private JsonNode changed;

    /**
     * Constructor to convert a byte stream shadow document to a JSON formatted shadow document.
     *
     * @param documentBytes The byte stream containing the shadow document.
     * @throws IOException if the byte stream cannot be serialized to a JsonNode.
     */
    public JsonShadowDocument(byte[] documentBytes) throws IOException {
        this.document = JsonUtil.getPayloadJson(documentBytes).orElse(null);
    }

    /**
     * Constructor to create a new JSON formatted shadow document from a JsonNode and store what change caused it.
     *
     * @param documentJson The JsonNode of the shadow document.
     * @param changed      The change that caused the new shadow document.
     */
    public JsonShadowDocument(JsonNode documentJson, JsonNode changed) {
        this.document = documentJson;
        this.changed = changed;
    }

    /**
     * Calculates the documents node based on the current version of the shadow document and the new updated
     * version of the shadow document sent in the update request.
     *
     * @param sourceDocument  The current version of the shadow documents.
     * @param currentDocument The updated version of the shadow document sent in the request.
     * @param clientToken     The client token if available in the shadow operation request.
     * @return return the new documents payload.
     * @throws IOException when the payload is not deserizable as JSON node.
     */
    public static byte[] createDocumentsPayload(JsonNode sourceDocument, JsonNode currentDocument,
                                                String clientToken) throws IOException {
        ObjectNode previousNode = JsonUtil.createObjectNode();
        JsonNode previousState = sourceDocument.get(SHADOW_DOCUMENT_STATE);
        JsonNode previousVersion = sourceDocument.get(SHADOW_DOCUMENT_VERSION);
        previousNode.set(SHADOW_DOCUMENT_STATE, previousState);
        previousNode.set(SHADOW_DOCUMENT_VERSION, previousVersion);

        ObjectNode currentNode = JsonUtil.createObjectNode();
        JsonNode currentState = currentDocument.get(SHADOW_DOCUMENT_STATE);
        JsonNode currentVersion = currentDocument.get(SHADOW_DOCUMENT_VERSION);
        currentNode.set(SHADOW_DOCUMENT_STATE, currentState);
        currentNode.set(SHADOW_DOCUMENT_VERSION, currentVersion);

        ObjectNode documentsNode = JsonUtil.createObjectNode();
        documentsNode.set(SHADOW_DOCUMENT_STATE_PREVIOUS, previousNode);
        documentsNode.set(SHADOW_DOCUMENT_STATE_CURRENT, currentNode);
        documentsNode.set(SHADOW_DOCUMENT_CLIENT_TOKEN, new TextNode(clientToken));
        setTimestamp(documentsNode);
        return JsonUtil.getPayloadBytes(documentsNode);
    }

    /**
     * Calculates the documents node based on the current version of the shadow document and the new updated
     * version of the shadow document sent in the update request.
     *
     * @param updateDocumentRequest The updated version of the shadow document sent in the request.
     * @return the new document.
     */
    public JsonShadowDocument createNewMergedDocument(JsonNode updateDocumentRequest) {
        //TODO: handler partial updated. Add a merge method.
        JsonNode previousState = document.get(SHADOW_DOCUMENT_STATE);
        if (isNullOrMissing(previousState)) {
            previousState = JsonUtil.createObjectNode();
        }
        ObjectNode currentNode = JsonUtil.createObjectNode();
        ObjectNode currentState = JsonUtil.createObjectNode();

        JsonNode updatedState = updateDocumentRequest.get(SHADOW_DOCUMENT_STATE);

        JsonNode updatedStateDesired = updatedState.get(SHADOW_DOCUMENT_STATE_DESIRED);
        if (!isNullOrMissing(updatedStateDesired)) {
            currentState.set(SHADOW_DOCUMENT_STATE_DESIRED, updatedStateDesired);
        } else if (!isNullOrMissing(previousState.get(SHADOW_DOCUMENT_STATE_DESIRED))) {
            currentState.set(SHADOW_DOCUMENT_STATE_DESIRED,
                    previousState.get(SHADOW_DOCUMENT_STATE_DESIRED));
        }
        JsonNode updatedStateReported = updatedState.get(SHADOW_DOCUMENT_STATE_REPORTED);
        if (!isNullOrMissing(updatedStateReported)) {
            currentState.set(SHADOW_DOCUMENT_STATE_REPORTED, updatedStateReported);
        } else if (!isNullOrMissing(previousState.get(SHADOW_DOCUMENT_STATE_REPORTED))) {
            currentState.set(SHADOW_DOCUMENT_STATE_REPORTED,
                    previousState.get(SHADOW_DOCUMENT_STATE_REPORTED));
        }
        currentNode.set(SHADOW_DOCUMENT_STATE, currentState);
        currentNode.set(SHADOW_DOCUMENT_VERSION, getUpdatedVersion(document));

        return new JsonShadowDocument(currentNode, updateDocumentRequest);
    }

    /**
     * Calculates the delta node based on the current version of the shadow document's desired and reported state.
     *
     * @param operation The operation for which the delta calculation is happening.
     * @return an optional value of the delta node.
     */
    public Optional<ObjectNode> delta(Operation operation) {
        return calculateDeltaNode(document, operation);
    }

    /**
     * Calculates the delta node based on the current version of the shadow document and the new updated
     * version of the shadow document.
     *
     * @param updatedDocument The updated version of the shadow document sent in the request.
     * @return an optional value of the delta node.
     */
    private Optional<ObjectNode> calculateDeltaNode(JsonNode updatedDocument, Operation operation) {
        // State node has to be present since we have already validated the payload before.
        JsonNode updatedState = updatedDocument.get(SHADOW_DOCUMENT_STATE);
        JsonNode updatedStateDesired = updatedState.get(SHADOW_DOCUMENT_STATE_DESIRED);

        // If there is no desired state in the update shadow request, there is no delta.
        if (isNullOrMissing(updatedStateDesired)) {
            return Optional.empty();
        }

        // State node has to be present since we have already validated the payload before putting it in the DB.
        JsonNode sourceState = document.get(SHADOW_DOCUMENT_STATE);
        JsonNode updatedStateReported = sourceState.get(SHADOW_DOCUMENT_STATE_REPORTED);

        // If there is no reported state in the update shadow request, there is no delta.
        if (isNullOrMissing(updatedStateReported)) {
            updatedStateReported = JsonUtil.createObjectNode();
        }

        JsonNode deltaNode = calculateDelta(updatedStateReported, updatedStateDesired);
        if (isNullOrMissing(deltaNode)) {
            return Optional.empty();
        }
        if (Operation.UPDATE_SHADOW.equals(operation)) {
            ObjectNode stateNode = JsonUtil.createObjectNode();
            stateNode.set(SHADOW_DOCUMENT_STATE, deltaNode);
            stateNode.set(SHADOW_DOCUMENT_VERSION, document.get(SHADOW_DOCUMENT_VERSION));
            setTimestamp(stateNode);
            return Optional.of(stateNode);
        }
        return Optional.of((ObjectNode) deltaNode);
    }


    private JsonNode calculateDelta(JsonNode original, JsonNode updated) {
        // If original and updated shadow documents are both objects then recursively diff
        if (original.isObject() && updated.isObject()) {
            final ObjectNode result = calculateDelta((ObjectNode) original, (ObjectNode) updated);
            return result.size() > 0 ? result : null;
        }

        // This handles the semantic number comparison.
        if (original.isNumber() && updated.isNumber()) {
            return original.asLong() != updated.asLong()
                    || original.asDouble() != updated.asDouble() ? updated : null;
        }

        // Now, if they both the original and updated shadow documents aren't objects or numbers, then they are either
        // both arrays, values, or differing types either way if they are different then they are completely different
        // and we can just return the updated node.
        if (!original.equals(updated)) {
            return updated;
        }

        // At this point, both the original and updated are same and we return null.
        return null;
    }

    private ObjectNode calculateDelta(final ObjectNode original, final ObjectNode updated) {
        final ObjectNode result = JsonUtil.createObjectNode();
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

    /**
     * Gets the new version for the updated shadow document from the current shadow document version.
     *
     * @param sourceJson The current shadow document.
     * @return The IntNode containing the new shadow document version
     */
    private IntNode getUpdatedVersion(JsonNode sourceJson) {
        JsonNode sourceVersion = sourceJson.get(SHADOW_DOCUMENT_VERSION);
        if (isNullOrMissing(sourceVersion)) {
            return new IntNode(0);
        }
        return new IntNode(sourceVersion.asInt() + 1);
    }

    /**
     * Sets the timestamp to the provided JsonNode.
     *
     * @param node The shadow document to which to add the client token if provided.
     */
    private static void setTimestamp(JsonNode node) {
        ((ObjectNode) node).set(SHADOW_DOCUMENT_TIMESTAMP, new LongNode(Instant.now().getEpochSecond()));
    }

    public JsonNode getVersionNode() {
        return document.get(SHADOW_DOCUMENT_VERSION);
    }
}
