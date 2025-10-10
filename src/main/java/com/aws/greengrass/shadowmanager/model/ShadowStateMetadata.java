/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.model;

import com.aws.greengrass.shadowmanager.util.JsonUtil;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.Getter;

import java.time.Clock;
import java.util.Iterator;
import java.util.Map;

import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_STATE_DESIRED;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_STATE_REPORTED;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_TIMESTAMP;
import static com.aws.greengrass.shadowmanager.util.JsonUtil.isNullOrMissing;
import static com.aws.greengrass.shadowmanager.util.JsonUtil.nullIfEmpty;

/**
 * Class for managing operations on the Shadow Document Metadata.
 */
@Getter
public class ShadowStateMetadata {
    @JsonProperty(SHADOW_DOCUMENT_STATE_DESIRED)
    private JsonNode desired;

    @JsonProperty(SHADOW_DOCUMENT_STATE_REPORTED)
    private JsonNode reported;
    private final Clock clock;

    public ShadowStateMetadata() {
        this(null, null, Clock.systemDefaultZone());
    }

    public ShadowStateMetadata(final JsonNode desired, final JsonNode reported) {
        this(desired, reported, Clock.systemDefaultZone());
    }

    ShadowStateMetadata(final JsonNode desired, final JsonNode reported, final Clock t) {
        this.desired = nullIfEmpty(desired);
        this.reported = nullIfEmpty(reported);
        this.clock = t;
    }

    /**
     * Creates a new instance of the shadow state by deep copying the desired and reported nodes.
     *
     * @return the new instance of the shadow state.
     */
    public ShadowStateMetadata deepCopy() {
        return new ShadowStateMetadata(
                isNullOrMissing(this.desired) ? this.desired : this.desired.deepCopy(),
                isNullOrMissing(this.reported) ? this.reported : this.reported.deepCopy());
    }

    /**
     * Updates the shadow metadata node's reported and desired JSON nodes from the current state node and the update
     * request state node.
     *
     * @param patch The update request patch.
     * @param state The state node in the shadow document.
     * @return The updated metadata node for the patch.
     */
    @SuppressWarnings("PMD.NullAssignment")
    public JsonNode update(JsonNode patch, ShadowState state) {
        // Create the patch metadata tree. This will transform nulls to metadata nodes.
        final JsonNode metadataPatchWithRemovedFields = createMetadataPatch(patch, false);
        final JsonNode metadataPatchWithoutRemovedFields = createMetadataPatch(patch, true);

        // If the thing now has null state after the update then the metadata should also be null
        if (state.isEmpty()) {
            desired = null;
            reported = null;
            return metadataPatchWithoutRemovedFields;
        }

        // Merge in the desired metadata
        final JsonNode patchDesired = metadataPatchWithRemovedFields.get(SHADOW_DOCUMENT_STATE_DESIRED);
        if (!isNullOrMissing(patchDesired)) {
            desired = nullIfEmpty(merge(state.getDesired(), desired, patchDesired));
        }

        // Merge in the reported metadata
        final JsonNode patchReported = metadataPatchWithRemovedFields.get(SHADOW_DOCUMENT_STATE_REPORTED);
        if (!isNullOrMissing(patchReported)) {
            reported = nullIfEmpty(merge(state.getReported(), reported, patchReported));
        }

        return metadataPatchWithoutRemovedFields;
    }

    private JsonNode createMetadataPatch(final JsonNode source, boolean removeFields) {
        // If the JsonNode is a NullNode then this field should be removed from the metadata
        if (source.isNull()) {
            return null;
        }

        if (source.isValueNode()) {
            ObjectNode node = JsonUtil.OBJECT_MAPPER.createObjectNode();
            node.set(SHADOW_DOCUMENT_TIMESTAMP, new LongNode(this.clock.instant().getEpochSecond()));
            return node;
        }

        if (source.isArray()) {
            final ArrayNode result = JsonUtil.OBJECT_MAPPER.createArrayNode();
            for (final JsonNode node : source) {
                result.add(createMetadataPatch(node, removeFields));
            }
            return result;
        }

        final ObjectNode result = JsonUtil.OBJECT_MAPPER.createObjectNode();
        final ObjectNode sourceObject = (ObjectNode) source;

        final Iterator<String> fieldIter = sourceObject.fieldNames();
        while (fieldIter.hasNext()) {
            final String fieldName = fieldIter.next();
            final JsonNode node = sourceObject.get(fieldName);
            JsonNode nodeMetadataPatch = createMetadataPatch(node, !removeFields);

            // If the field isn't being removed then recurse
            if (!removeFields || nodeMetadataPatch != null) {
                result.set(fieldName, nodeMetadataPatch);
            }
        }
        return result;
    }

    private JsonNode merge(final JsonNode state, final JsonNode metadata, final JsonNode patch) {
        // If the state is null then there should be no metadata
        if (isNullOrMissing(state)) {
            return null;
        }

        //if the metadata node is null, then lets create an empty one so we can merge into it
        final JsonNode mergeNode = metadata == null ? JsonUtil.OBJECT_MAPPER.createObjectNode() : metadata;

        merge((ObjectNode) state, (ObjectNode) mergeNode, (ObjectNode) patch);
        return mergeNode;
    }

    private void merge(final ObjectNode state, final ObjectNode metadata, final ObjectNode patch) {
        final Iterator<Map.Entry<String, JsonNode>> fieldIter = patch.fields();
        while (fieldIter.hasNext()) {
            final Map.Entry<String, JsonNode> patchFieldEntry = fieldIter.next();
            final String patchFieldName = patchFieldEntry.getKey();
            final JsonNode patchFieldNode = patchFieldEntry.getValue();

            JsonNode metadataFieldNode = metadata.get(patchFieldName);
            final JsonNode stateFieldNode = state.get(patchFieldName);

            // If the state doesn't have the node then it was removed from state and should be
            // removed from metadata if present. If it's an object then leave it as empty.
            if (patchFieldNode == null || patchFieldNode.isNull()) {
                metadata.remove(patchFieldName);
                continue;
            }

            // If the patch is an array, we can replace the original with the patch.
            if (patchFieldNode.isArray()) {
                if (isNullOrMissing(metadataFieldNode)) {
                    metadataFieldNode = JsonUtil.OBJECT_MAPPER.createArrayNode();
                } else {
                    ((ArrayNode) metadataFieldNode).removeAll();
                }
                ((ArrayNode) metadataFieldNode).addAll((ArrayNode) patchFieldNode);
                metadata.set(patchFieldName, metadataFieldNode);
                continue;
            }

            // If the metadata node doesn't exist then copy the patch into it.
            if (isNullOrMissing(metadataFieldNode)) {
                metadataFieldNode = patchFieldNode.deepCopy(); //deep copy here to prevent modifying patch
                metadata.set(patchFieldName, metadataFieldNode);
                // Not continuing here because we will want to recurse over the patch fields to make sure there are
                // no null nodes in the state node that need to be removed.
            }

            // If there is a type mismatch then the patch type always wins. This allows nodes to switch between
            // values, arrays and objects and metadata stays in sync
            if (patchFieldNode.getNodeType() != metadataFieldNode.getNodeType()) {
                metadataFieldNode = patchFieldNode.deepCopy(); //deep copy here to prevent modifying patch
                metadata.set(patchFieldName, metadataFieldNode);
                // Not continuing here because we will want to recurse over the patch fields to make sure there are
                // no null nodes in the state node that need to be removed.
            }

            // If the patchField is a metadata node then we can replace the original node with the patch.
            if (isMetadataNode(patchFieldNode)) {
                metadata.set(patchFieldName, patchFieldNode);
                continue;
            }

            // If the original field is a metadata node then the type of the original data has changed since patch
            // is not a metadata node and we can replace it.
            if (isMetadataNode(metadataFieldNode)) {
                metadata.set(patchFieldName, patchFieldNode);
                // Not continuing here because we will want to recurse over the patch fields to make sure there are
                // no null nodes in the state node that need to be removed.
            }

            // Now we have gotten to the case where the original and patch nodes are the same type and are not
            // metadata nodes, recurse if not an empty object.
            if (patchFieldNode.isObject()) {
                // If the patch is an empty object, then the metadata should be set to an empty object.
                if (patchFieldNode.isEmpty()) {
                    metadata.set(patchFieldName, patchFieldNode);
                } else {
                    merge((ObjectNode) stateFieldNode, (ObjectNode) metadataFieldNode, (ObjectNode) patchFieldNode);
                }
            }
        }
    }

    /**
     * Checks whether a Json node is a metadata node or not. A metadata node should be an object and have only one field
     * which should be a long node containing the timestamp.
     *
     * @param node the metadata node to check.
     * @return true if the node follows all the metadata criteria; Else false.
     */
    public static boolean isMetadataNode(final JsonNode node) {
        // Metadata nodes are object that contain only a timestamp
        if (!node.isObject() || node.size() != 1) {
            return false;
        }

        final JsonNode timestamp = node.get(SHADOW_DOCUMENT_TIMESTAMP);
        if (isNullOrMissing(timestamp)) {
            return false;
        }

        return timestamp.isIntegralNumber();
    }

    /**
     * Converts the class to its JSON representation.
     *
     * @return a JSON node containing the shadow state.
     */
    public JsonNode toJson() {
        final ObjectNode result = JsonUtil.OBJECT_MAPPER.createObjectNode();
        if (this.desired != null) {
            result.set(SHADOW_DOCUMENT_STATE_DESIRED, this.desired);
        }
        if (this.reported != null) {
            result.set(SHADOW_DOCUMENT_STATE_REPORTED, this.reported);
        }
        return result;
    }

    /**
     * Gets the metadata node for the delta node based on the desired node.
     *
     * @param delta The delta node.
     * @return the metadata node for the delta.
     */
    public JsonNode getDeltaMetadata(JsonNode delta) {
        if (isNullOrMissing(delta)) {
            return null;
        }

        return buildMetadata(delta, desired);
    }

    /**
     * Builds the metadata node for the delta node based on the desired node. Based on the fields in the desired, get
     * the metadata node.
     *
     * @param deltaNode    The delta node.
     * @param metadataNode The metadata node.
     * @return the metadata node based on the delta node.
     */
    private JsonNode buildMetadata(final JsonNode deltaNode, final JsonNode metadataNode) {
        // If the deltaNode is a value then return the metadata associated with it
        if (deltaNode.isValueNode()) {
            return metadataNode;
        }

        // If the deltaNode is an array then recurse on each index
        if (deltaNode.isArray()) {
            final ArrayNode result = JsonUtil.OBJECT_MAPPER.createArrayNode();
            final ArrayNode deltaArray = (ArrayNode) deltaNode;
            final ArrayNode metadataArray = (ArrayNode) metadataNode;

            for (int i = 0; i < deltaArray.size(); ++i) {
                final JsonNode deltaChild = deltaArray.get(i);
                final JsonNode metadataChild = metadataArray.get(i);
                result.add(buildMetadata(deltaChild, metadataChild));
            }
            return result;
        }

        // If the deltaNode is an object then recurse on each field
        final ObjectNode result = JsonUtil.OBJECT_MAPPER.createObjectNode();
        final ObjectNode metadataObjectNode = (ObjectNode) metadataNode;
        final ObjectNode deltaObjectNode = (ObjectNode) deltaNode;

        final Iterator<String> fieldNames = deltaObjectNode.fieldNames();
        while (fieldNames.hasNext()) {
            final String fieldName = fieldNames.next();
            final JsonNode deltaField = deltaObjectNode.get(fieldName);
            final JsonNode metadataField = metadataObjectNode.get(fieldName);
            result.set(fieldName, buildMetadata(deltaField, metadataField));
        }
        return result;
    }

    /**
     * Gets the latest updated timestamp from the metadata reported and desired nodes.
     *
     * @return the latest timestamp from the reported and desired nodes.
     */
    public long getLatestUpdatedTimestamp() {
        long desiredMax = 0L;
        if (desired != null && desired.isObject()) {
            desiredMax = getLatestUpdatedTimestamp((ObjectNode) desired);
        }
        long reportedMax = 0L;
        if (reported != null && reported.isObject()) {
            reportedMax = getLatestUpdatedTimestamp((ObjectNode) reported);
        }
        long overAllMax = Math.max(desiredMax, reportedMax);
        return overAllMax == 0 ? this.clock.instant().getEpochSecond() : overAllMax;
    }

    private long getLatestUpdatedTimestamp(JsonNode node) {
        long maxVal = 0L;
        if (isNullOrMissing(node)) {
            return maxVal;
        }
        if (node.isObject() && !isMetadataNode(node)) {
            maxVal = getLatestUpdatedTimestamp((ObjectNode) node);
        } else if (node.isArray()) {
            maxVal = getLatestUpdatedTimestamp((ArrayNode) node);
        } else if (node.isObject() && isMetadataNode(node)) {
            maxVal = node.get(SHADOW_DOCUMENT_TIMESTAMP).asLong();
        }
        return maxVal;
    }

    private long getLatestUpdatedTimestamp(ArrayNode node) {
        long overAllMaxVal = 0;
        for (JsonNode arrayNode : node) {
            long maxVal = getLatestUpdatedTimestamp(arrayNode);

            if (overAllMaxVal < maxVal) {
                overAllMaxVal = maxVal;
            }
        }
        return overAllMaxVal;
    }

    private long getLatestUpdatedTimestamp(ObjectNode node) {
        long overAllMaxVal = 0;
        final Iterator<Map.Entry<String, JsonNode>> fieldIter = node.fields();
        while (fieldIter.hasNext()) {
            final Map.Entry<String, JsonNode> entry = fieldIter.next();
            final JsonNode entryNode = entry.getValue();
            long maxVal = getLatestUpdatedTimestamp(entryNode);

            if (overAllMaxVal < maxVal) {
                overAllMaxVal = maxVal;
            }
        }
        return overAllMaxVal;
    }
}
