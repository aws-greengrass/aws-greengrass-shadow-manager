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

import static com.aws.greengrass.shadowmanager.util.JsonUtil.isNullOrMissing;

/**
 * Handles merge of two JsonNode objects.
 */
public final class JsonMerger {
    private JsonMerger() {
    }

    /**
     * Merges the patch JSON node to the existing source JSON node. If the node already exists in the source, then
     * it replaces it. If the node is an array, then the the source array's contents are overwritten with the contents
     * from the patch.
     *
     * @param source The source JSON to merge.
     * @param patch  The patch JSON.
     * @throws InvalidRequestParametersException If the source and patch node are of different types.
     */
    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST", justification = "We do check the type before cast.")
    public static void merge(JsonNode source, final JsonNode patch) throws InvalidRequestParametersException {
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
}
