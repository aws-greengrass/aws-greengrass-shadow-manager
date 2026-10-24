/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.util;

import com.aws.greengrass.shadowmanager.exception.InvalidRequestParametersException;
import com.aws.greengrass.shadowmanager.model.ErrorMessage;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Iterator;
import java.util.Optional;

import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_STATE;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_STATE_DESIRED;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_STATE_REPORTED;
import static com.aws.greengrass.shadowmanager.util.JsonUtil.isNullOrMissing;

/**
 * Handles merge of two JsonNode objects.
 */
public final class JsonMerger {
    // Marks a change that a patch can't express; tested with isMissingNode().
    private static final JsonNode INEXPRESSIBLE = MissingNode.getInstance();

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

    /**
     * Applies the state node of an update request to a state node like IoT Device Shadow: a null state clears both
     * sections, and each section in the update is merged with {@link #mergeShadowState(JsonNode, JsonNode)}.
     *
     * @param state  the current state node, with desired and reported; may be null. It isn't modified.
     * @param update the state node of the update request.
     * @return the state node after the update, without empty sections.
     */
    public static ObjectNode applyShadowStateUpdate(final JsonNode state, final JsonNode update) {
        final ObjectNode result = JsonUtil.OBJECT_MAPPER.createObjectNode();
        if (update == null || update.isNull()) {
            return result;
        }
        for (String section : new String[]{SHADOW_DOCUMENT_STATE_DESIRED, SHADOW_DOCUMENT_STATE_REPORTED}) {
            JsonNode current = section(state, section);
            current = current == null ? null : current.deepCopy();
            final JsonNode merged = update.has(section) ? mergeShadowState(current, update.get(section)) : current;
            if (merged != null) {
                result.set(section, merged);
            }
        }
        return result;
    }

    /**
     * Computes the update that turns one shadow state into another when applied with
     * {@link #mergeShadowState(JsonNode, JsonNode)}: changed values and objects are set, removed fields are set to
     * null, a removed section is set to null, and objects are diffed field by field.
     *
     * <p>A patch can't add or remove a null stored inside an object, or add an object with nulls to a missing field.
     * If the change needs that, no update is returned.
     *
     * @param from the current state node, with desired and reported; may be null.
     * @param to   the state node to reach; may be null.
     * @return the state node of the update, empty if nothing changed, or no value if a patch can't express it.
     */
    public static Optional<ObjectNode> diffShadowState(final JsonNode from, final JsonNode to) {
        final ObjectNode patch = JsonUtil.OBJECT_MAPPER.createObjectNode();
        for (String section : new String[]{SHADOW_DOCUMENT_STATE_DESIRED, SHADOW_DOCUMENT_STATE_REPORTED}) {
            final JsonNode diff = diffSection(section(from, section), section(to, section));
            if (diff != null && diff.isMissingNode()) {
                return Optional.empty();
            }
            if (diff != null) {
                patch.set(section, diff);
            }
        }
        return Optional.of(patch);
    }

    private static JsonNode section(final JsonNode state, final String section) {
        if (state == null || !state.isObject()) {
            return null;
        }
        return JsonUtil.nullIfEmpty(state.get(section));
    }

    private static JsonNode diffSection(final JsonNode from, final JsonNode to) {
        if (to == null) {
            return from == null ? null : NullNode.getInstance();
        }
        if (from == null) {
            return hasNullLeaf(to) ? INEXPRESSIBLE : to.deepCopy();
        }
        final JsonNode diff = diffObject((ObjectNode) from, (ObjectNode) to);
        return !diff.isMissingNode() && diff.isEmpty() ? null : diff;
    }

    private static JsonNode diffObject(final ObjectNode from, final ObjectNode to) {
        final ObjectNode patch = JsonUtil.OBJECT_MAPPER.createObjectNode();
        final Iterator<String> removed = from.fieldNames();
        while (removed.hasNext()) {
            final String field = removed.next();
            if (!to.has(field)) {
                if (from.get(field).isNull()) {
                    return INEXPRESSIBLE;
                }
                patch.set(field, NullNode.getInstance());
            }
        }
        final Iterator<String> fields = to.fieldNames();
        while (fields.hasNext()) {
            final String field = fields.next();
            final JsonNode fromValue = from.get(field);
            final JsonNode toValue = to.get(field);
            if (toValue.equals(fromValue)) {
                continue;
            }
            if (toValue.isNull() || (fromValue == null || fromValue.isNull()) && hasNullLeaf(toValue)) {
                return INEXPRESSIBLE;
            }
            if (fromValue != null && fromValue.isObject() && toValue.isObject()) {
                final JsonNode diff = diffObject((ObjectNode) fromValue, (ObjectNode) toValue);
                if (diff.isMissingNode()) {
                    return INEXPRESSIBLE;
                }
                if (!diff.isEmpty()) {
                    patch.set(field, diff);
                }
                continue;
            }
            patch.set(field, toValue.deepCopy());
        }
        return patch;
    }

    private static boolean hasNullLeaf(final JsonNode node) {
        if (!node.isObject()) {
            return false;
        }
        final Iterator<JsonNode> values = node.elements();
        while (values.hasNext()) {
            final JsonNode value = values.next();
            if (value.isNull() || hasNullLeaf(value)) {
                return true;
            }
        }
        return false;
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
