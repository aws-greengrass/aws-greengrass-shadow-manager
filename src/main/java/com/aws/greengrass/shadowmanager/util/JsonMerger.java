/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.util;

import com.aws.greengrass.shadowmanager.exception.InvalidRequestParametersException;
import com.aws.greengrass.shadowmanager.model.ErrorMessage;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Iterator;

import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_STATE;
import static com.aws.greengrass.shadowmanager.util.JsonUtil.isNullOrMissing;

/**
 * Handles merge of two JsonNode objects.
 */
public final class JsonMerger {
    private JsonMerger() {
    }

    /**
     * Merges the patch JSON node to the existing source JSON node. If the node already exists in the source, then
     * it replaces it. If the node is an array, then the source array's contents are overwritten with the contents
     * from the patch.
     *
     * @param source The source JSON to merge.
     * @param patch  The patch JSON.
     * @throws InvalidRequestParametersException If the source and patch node are of different types.
     */
    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST", justification = "We do check the type before cast.")
    public static void merge(JsonNode source, final JsonNode patch) throws InvalidRequestParametersException {
        if (JsonUtil.isNullStateDocument(patch)) {
            clearState(source);
            return;
        }

        if (JsonUtil.isEmptyStateDocument(patch)) {
            return;
        }

        // If both nodes are objects then do a recursive patch
        if (source.isObject() && patch.isObject()) {
            merge((ObjectNode) source, (ObjectNode) patch);
            return;
        }

        // If both nodes are arrays then overwrite the source array's contents
        if (source.isArray() && patch.isArray()) {
            ((ArrayNode) source).removeAll();
            ((ArrayNode) source).addAll((ArrayNode) patch);
            return;
        }

        // Both the source and patch node needs to be of the same type inorder to merge.
        throw new InvalidRequestParametersException(ErrorMessage.createInvalidPayloadJsonMessage("Merge only works with"
                + " Json objects whose underlying node are the same type."));
    }

    private static void merge(final ObjectNode source, final ObjectNode patch) {
        final Iterator<String> fieldNames = patch.fieldNames();
        while (fieldNames.hasNext()) {
            final String field = fieldNames.next();

            final JsonNode originalValue = source.get(field);
            final JsonNode patchValue = patch.get(field);

            // If the patch value is set to null then remove the original node if present
            if (isNullOrMissing(patchValue)) {
                if (!isNullOrMissing(originalValue)) {
                    source.remove(field);
                }
                continue;
            }

            // If the source doesn't have the value then add the new node.
            if (isNullOrMissing(originalValue)) {
                if (patchValue.isObject()) {
                    final ObjectNode child = createMergeTree((ObjectNode) patchValue);
                    // If the child contains non-null elements, add it to the source.
                    if (child != null) {
                        source.set(field, child);
                    }
                } else {
                    source.set(field, patchValue);
                }
                continue;
            }

            // If the nodes are objects then recurse and merge them.
            if (originalValue.isObject() && patchValue.isObject()) {
                merge((ObjectNode) originalValue, (ObjectNode) patchValue);
                continue;
            }

            // If sourceValue and patchValue are not objects then we just update the source field
            // to point the new patch value.
            source.set(field, patchValue);
        }
    }

    /**
     * Creates a tree that can be merged into a JsonNode.  Does the following:
     * 1. removes nulls
     * 2. collapses empty objects after nulls are removed
     * 3. Creates a copy of the tree because the node will be modified
     *
     * @param node The object node to convert.
     * @return null if all the nodes in the node are null; Else a merged object node.
     */
    private static ObjectNode createMergeTree(final ObjectNode node) {
        final ObjectNode result = JsonUtil.OBJECT_MAPPER.createObjectNode();
        final Iterator<String> fieldIter = node.fieldNames();

        boolean isNullNodePresent = false;
        while (fieldIter.hasNext()) {
            final String field = fieldIter.next();
            final JsonNode value = node.get(field);

            if (isNullOrMissing(value)) {
                isNullNodePresent = true;
                continue;
            }

            if (value.isObject()) {
                final ObjectNode child = createMergeTree((ObjectNode) value);
                // If the child contains non-null elements, add it to the source.
                if (child == null) {
                    isNullNodePresent = true;
                } else {
                    result.set(field, child);
                }
                continue;
            }

            result.set(field, value);
        }

        if (isNullNodePresent && result.size() == 0) {
            return null;
        }

        return result;
    }

    /**
     * Merges the desired or reported node of an update request into the current one, like IoT Device Shadow. Unlike
     * {@link #merge(JsonNode, JsonNode)}, an empty object is a value, not null, and a key named "state" is an ordinary
     * key.
     * <ul>
     *     <li>a null removes the field; a null already stored is left as is.</li>
     *     <li>an object is merged into an existing object; an empty object changes nothing.</li>
     *     <li>an object for a missing or null field is added without its null leaves, and is skipped if nothing is
     *     left; empty objects in it are kept.</li>
     *     <li>an object replacing a value or an array, and any value or array, replaces the field as is.</li>
     * </ul>
     * Objects left empty after their fields are removed are kept.
     *
     * @param current the current desired or reported node; may be null.
     * @param patch   the desired or reported node in the update request; null removes the node.
     * @return the merged node, or null if it is empty.
     */
    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST", justification = "The update schema only allows objects.")
    public static JsonNode mergeShadowState(final JsonNode current, final JsonNode patch) {
        if (patch == null || patch.isNull()) {
            return null;
        }
        final ObjectNode merged = current != null && current.isObject()
                ? (ObjectNode) current : JsonUtil.OBJECT_MAPPER.createObjectNode();
        mergeShadowState(merged, (ObjectNode) patch);
        return JsonUtil.nullIfEmpty(merged);
    }

    private static void mergeShadowState(final ObjectNode source, final ObjectNode patch) {
        final Iterator<String> fieldNames = patch.fieldNames();
        while (fieldNames.hasNext()) {
            final String field = fieldNames.next();
            final JsonNode sourceValue = source.get(field);
            final JsonNode patchValue = patch.get(field);

            if (patchValue.isNull()) {
                if (sourceValue != null && !sourceValue.isNull()) {
                    source.remove(field);
                }
                continue;
            }
            if (patchValue.isObject()) {
                if (sourceValue != null && sourceValue.isObject()) {
                    mergeShadowState((ObjectNode) sourceValue, (ObjectNode) patchValue);
                } else if (sourceValue == null || sourceValue.isNull()) {
                    final ObjectNode added = withoutNulls((ObjectNode) patchValue);
                    if (added != null) {
                        source.set(field, added);
                    }
                } else {
                    source.set(field, patchValue.deepCopy());
                }
                continue;
            }
            source.set(field, patchValue.deepCopy());
        }
    }

    /**
     * Copies the object without its null leaves. Objects emptied by the removal are dropped; empty objects in the
     * input are kept.
     *
     * @return the copy, or null if every leaf was null.
     */
    private static ObjectNode withoutNulls(final ObjectNode node) {
        final ObjectNode result = JsonUtil.OBJECT_MAPPER.createObjectNode();
        boolean removed = false;
        final Iterator<String> fieldNames = node.fieldNames();
        while (fieldNames.hasNext()) {
            final String field = fieldNames.next();
            final JsonNode value = node.get(field);
            final JsonNode copy = value.isObject() ? withoutNulls((ObjectNode) value) : value.deepCopy();
            if (copy == null || copy.isNull()) {
                removed = true;
            } else {
                result.set(field, copy);
            }
        }
        return removed && result.isEmpty() ? null : result;
    }

    private static void clearState(JsonNode node) {
        if (!node.isObject()) {
            return;
        }
        JsonNode stateNode = node.get(SHADOW_DOCUMENT_STATE);
        if (stateNode != null) {
            clearState(stateNode);
            return;
        }
        ((ObjectNode) node).removeAll();
    }
}
