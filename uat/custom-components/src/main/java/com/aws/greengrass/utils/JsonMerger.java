/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.utils;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class JsonMerger {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private JsonMerger() {
    }

    public static void merge(final JsonNode source, final JsonNode patch) {

        //ValueNodes are immutable, thus we cannot merge a new value into it.
        if (source.isValueNode()) {
            throw new IllegalStateException("Cannot merge update a value node.  ValueNodes are immutable");
        }

        //if both nodes are objects then do a recursive patch
        if (source.isObject() && patch.isObject()) {
            merge((ObjectNode) source, (ObjectNode) patch);
            return;
        }

        //if both nodes are arrays then overwrite the source array's contents
        if (source.isArray() && patch.isArray()) {
            ((ArrayNode) source).removeAll();
            ((ArrayNode) source).addAll((ArrayNode) patch);
            return;
        }

        throw new IllegalArgumentException("Merge only works with Json objects whose underlying node are the same type.");

    }

    private static void merge(final ObjectNode source, final ObjectNode patch) {

        final Iterator<String> fieldIter = patch.fieldNames();
        while (fieldIter.hasNext()) {
            final String field = fieldIter.next();

            final JsonNode originalValue = source.get(field);
            final JsonNode patchValue = patch.get(field);

            //if the patch value is set to null then wipe out the value from original if present
            if (isNull(patchValue)) {
                if (!isNull(originalValue)) {
                    source.remove(field);
                }
                continue;
            }

            //if the source doesn't have the value then add it
            if (isNull(originalValue)) {
                if (patchValue.isObject()) {
                    final ObjectNode child = createMergeTree((ObjectNode) patchValue);
                    if (child != null) {
                        source.set(field, child);
                    }
                } else {
                    //TODO this will allow arrays to leak wtih null values in them.  We need
                    //to re-think how to handle arrays
                    source.set(field, patchValue);
                }
                continue;
            }

            //if we got objects then recurse
            if (originalValue.isObject() && patchValue.isObject()) {
                merge((ObjectNode) originalValue, (ObjectNode) patchValue);
                continue;
            }

            //If sourceValue and patchValue are not objects then we just update the source field
            //to point the new patch value.
            source.set(field, patchValue);
        }
    }

    static void merge(final JsonNode source, final JsonPointer pointer, final JsonNode patch) {

        //Get the parent node so we can remap it to point to the new json
        final JsonNode parent = source.at(pointer.head());

        if (parent.isObject()) {

            final ObjectNode parentObjectNode = (ObjectNode) parent;
            final String fieldName = pointer.last().getMatchingProperty();
            final JsonNode mergeNode = parent.get(fieldName);

            if (isNull(patch)) {
                parentObjectNode.remove(fieldName);
                return;
            }

            if (isNull(mergeNode)) {
                parentObjectNode.set(fieldName, patch);
                return;
            }

            if (mergeNode.isObject() && patch.isObject()) {
                merge((ObjectNode) mergeNode, (ObjectNode) patch);
                return;
            }

            parentObjectNode.set(fieldName, patch);
        }

        //if the parent is an array update the index to pointer to the new value
        if (parent.isArray()) {
            final int index = Integer.parseInt(pointer.last().getMatchingProperty());
            final JsonNode mergeNode = parent.get(index);
            if (mergeNode == null) {
                //in parent array increment index
                //don't support range increments
                ((ArrayNode) parent).add(patch);
            } else {
                if (mergeNode.isObject() && patch.isObject()) {
                    merge((ObjectNode) mergeNode, (ObjectNode) patch);
                } else {
                    ((ArrayNode) parent).set(index, patch);
                }
            }
        }

        //we have arrived at a place where the pointer does not exist in the document and merge was called
        if (parent.isMissingNode()) {

            //traverse the pointer fields and create nodes in a new document,
            //eventually setting the last pointer via a patch,
            //then merge the newly created document with the old
            final List<String> segments = new LinkedList<String>(Arrays.asList(pointer.toString().split("/")));

            //remove the empty field as a result of root pointer /.
            segments.remove(0);

            ObjectNode currentNode = (ObjectNode) source;
            for (int i = 0; i < segments.size(); ++i) {
                final String segment = segments.get(i);
                if (currentNode.get(segment) == null) {
                    //create an empty object
                    currentNode.set(segment, MAPPER.createObjectNode());
                }
                if (i == (segments.size() - 1)) {
                    currentNode.set(segment, patch);
                } else {
                    currentNode = (ObjectNode) currentNode.get(segment);
                }
            }

        }

    }

    /**
     * Creates a tree that can be merged into a JsonNode.  Does the following:
     * 1. removes nulls
     * 2. collapses empty objects after nulls are removed
     * 3. Creates a copy of the tree because the node will be modified
     */
    private static ObjectNode createMergeTree(final ObjectNode node) {
        final ObjectNode result = MAPPER.createObjectNode();
        final Iterator<String> fieldIter = node.fieldNames();

        boolean containedNullNode = false;
        while (fieldIter.hasNext()) {
            final String field = fieldIter.next();
            final JsonNode value = node.get(field);

            if (isNull(value)) { //skip null values
                containedNullNode = true;
                continue;
            }

            if (value.isObject()) {
                final ObjectNode child = createMergeTree((ObjectNode) value);

                if (child != null) {
                    result.set(field, child);
                } else {
                    containedNullNode = true;
                }
                continue;
            }

            //TODO this has problems with arrays that contain nulls, but punting on that for now
            result.set(field, value);
        }

        if (containedNullNode && result.size() == 0) {
            return null;
        }

        return result;
    }

    private static boolean isNull(final JsonNode node) {
        return node == null || node.isNull();
    }
}
