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
        final JsonNode patchMetadata = createPatchMetadata(patch);
        final boolean patchIsObject = patch != null && patch.isObject();
        desired = sectionMetadata(state.getDesired(), desired,
                patchIsObject ? patch.get(SHADOW_DOCUMENT_STATE_DESIRED) : null);
        reported = sectionMetadata(state.getReported(), reported,
                patchIsObject ? patch.get(SHADOW_DOCUMENT_STATE_REPORTED) : null);
        return patchMetadata;
    }

    // https://docs.aws.amazon.com/iot/latest/developerguide/device-shadow-document.html
    private JsonNode createPatchMetadata(final JsonNode source) {
        if (source.isArray()) {
            final ArrayNode result = JsonUtil.OBJECT_MAPPER.createArrayNode();
            for (final JsonNode node : source) {
                result.add(createPatchMetadata(node));
            }
            return result;
        }
        if (source.isObject()) {
            final ObjectNode result = JsonUtil.OBJECT_MAPPER.createObjectNode();
            final Iterator<Map.Entry<String, JsonNode>> fieldIter = source.fields();
            while (fieldIter.hasNext()) {
                final Map.Entry<String, JsonNode> field = fieldIter.next();
                result.set(field.getKey(), createPatchMetadata(field.getValue()));
            }
            return result;
        }
        return timestampNode();
    }

    private JsonNode sectionMetadata(final JsonNode state, final JsonNode metadata, final JsonNode patch) {
        if (isNullOrMissing(state)) {
            return null;
        }
        if (patch == null) {
            return metadata;
        }
        return nullIfEmpty(buildStateMetadata(state, metadata, patch));
    }

    // Metadata has the shape of the updated state; see the PR description.
    private JsonNode buildStateMetadata(final JsonNode state, final JsonNode metadata, final JsonNode patch) {
        final boolean touched = patch != null;
        if (state.isObject()) {
            final boolean hasChildMetadata = metadata != null && metadata.isObject() && !isMetadataNode(metadata);
            final ObjectNode result = JsonUtil.OBJECT_MAPPER.createObjectNode();
            final Iterator<Map.Entry<String, JsonNode>> fieldIter = state.fields();
            while (fieldIter.hasNext()) {
                final Map.Entry<String, JsonNode> field = fieldIter.next();
                final JsonNode childPatch = touched && patch.isObject() ? patch.get(field.getKey()) : patch;
                final JsonNode childMetadata = hasChildMetadata ? metadata.get(field.getKey()) : null;
                result.set(field.getKey(), buildStateMetadata(field.getValue(), childMetadata, childPatch));
            }
            return result;
        }
        if (state.isArray()) {
            if (touched || metadata == null || !metadata.isArray() || metadata.size() != state.size()) {
                return createPatchMetadata(state);
            }
            final ArrayNode result = JsonUtil.OBJECT_MAPPER.createArrayNode();
            for (int i = 0; i < state.size(); i++) {
                result.add(buildStateMetadata(state.get(i), metadata.get(i), null));
            }
            return result;
        }
        if (!touched && metadata != null && isMetadataNode(metadata)) {
            return metadata;
        }
        return timestampNode();
    }

    private ObjectNode timestampNode() {
        final ObjectNode node = JsonUtil.OBJECT_MAPPER.createObjectNode();
        node.set(SHADOW_DOCUMENT_TIMESTAMP, new LongNode(this.clock.instant().getEpochSecond()));
        return node;
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

        // If there is no metadata for this subtree, there is nothing to build. This can happen when the delta and the
        // desired metadata are structurally inconsistent (e.g. a delta node exists at a path where the desired metadata
        // is null/missing). Guard against it here instead of dereferencing null in the array/object handling below.
        if (isNullOrMissing(metadataNode)) {
            return null;
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
