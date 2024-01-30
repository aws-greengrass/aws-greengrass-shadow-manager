/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.util;

import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static com.aws.greengrass.shadowmanager.util.JsonUtil.OBJECT_MAPPER;
import static com.aws.greengrass.shadowmanager.util.JsonUtil.isNullOrMissing;

/**
 * Class to merge the current local shadow, current cloud shadow and the last synced shadow.
 */
public final class SyncNodeMerger {
    private static final Logger logger = LogManager.getLogger(SyncNodeMerger.class);

    private SyncNodeMerger() {
    }

    /**
     * Gets the merged node for sync based on the current local document, current cloud document, last synced document
     * and whose data should be considered as primary in a conflict situation.
     *
     * @param local The current local document
     * @param cloud The current cloud document
     * @param base  The last synced document
     * @param owner The data owner which is used during conflict resolution
     * @return The correct synchronized merge node.
     */
    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST", justification = "We do check the type before cast.")
    public static JsonNode getMergedNode(JsonNode local, JsonNode cloud, JsonNode base, DataOwner owner) {

        // If both the local and cloud are null, return null since they are the same.
        if (isNullOrMissing(local) && isNullOrMissing(cloud)) {
            return null;
        }

        // Check if all the 3 versions are objects. If so, iterate over them and figure out the exact merged value
        // for the object node.
        if (areNodesObjects(local, cloud, base)) {
            final ObjectNode result = getMergedNode((ObjectNode)local, (ObjectNode)cloud, (ObjectNode)base, owner);
            return result.size() > 0 ? result : null;
        } else {
            // Check if the local value has changed since the last sync.
            boolean hasLocalChanged = compare(local, base);
            // Check if the cloud value has changed since the last sync.
            boolean hasCloudChanged = compare(cloud, base);

            // If both local and cloud versions have a different value from the last sync for this node, resolve the
            // conflict based on the owner.
            if (hasCloudChanged && hasLocalChanged) {
                logger.atDebug()
                        .kv("local", local == null ? "" : local.toString())
                        .kv("cloud", cloud == null ? "" : cloud.toString())
                        .log("Conflict found, choosing the value for {}", owner);
                return resolveConflict(local, cloud, owner);
            } else if (hasCloudChanged) {
                return cloud;
            } else if (hasLocalChanged) {
                return local;
            } else {
                return chooseOwnerValue(local, cloud, owner);
            }
        }
    }

    /**
     * This function handles the recursion for all the fields in either an ObjectNode to figure out
     * the synchronized merged node.
     *
     * @param local The current local document.
     * @param cloud The current cloud document.
     * @param base  The last synced document.
     * @param owner The data owner which is used during conflict resolution.
     * @return The correct synchronized merge node.
     */
    private static ObjectNode getMergedNode(ObjectNode local, ObjectNode cloud, ObjectNode base,
                                            DataOwner owner) {
        ObjectNode result = OBJECT_MAPPER.createObjectNode();
        final HashSet<String> visited = new HashSet<>();
        iterateOverAllUnvisitedFields(local, cloud, base, owner, result, visited, local.fieldNames());
        iterateOverAllUnvisitedFields(local, cloud, base, owner, result, visited, cloud.fieldNames());
        return result;
    }

    private static boolean areNodesObjects(JsonNode local, JsonNode cloud, JsonNode base) {
        return local != null && local.isObject() && cloud != null && cloud.isObject() && base != null
                && base.isObject();
    }

    /**
     * Compare the 2 JSON nodes. First perform semantic number comparison. If they are not numbers, then check if the
     * local and base are different.
     *
     * @param latest The latest JSON node
     * @param base   The base JSON node.
     * @return true if the 2 JSON nodes are not equal; Else false.
     */
    @SuppressWarnings("PMD.ConfusingTernary")
    private static boolean compare(JsonNode latest, JsonNode base) {
        boolean hasChanged;
        if (latest != null && latest.isNumber() && base != null && base.isNumber()) {
            hasChanged = !(latest.asLong() == base.asLong() && latest.asDouble() == base.asDouble());
        } else if (latest != null && !latest.equals(base)) {
            hasChanged = true;
        } else {
            hasChanged = base != null && latest == null;
        }
        return hasChanged;
    }

    /**
     * Iterates over all the unvisited nodes and gets the correct synchronized node for all the nodes.
     *
     * @param local   The current local document.
     * @param cloud   The current cloud document.
     * @param base    The last synced document.
     * @param owner   The data owner which is used during conflict resolution.
     * @param result  The result synchronized merged node.
     * @param visited The set of visited nodes.
     * @param fields  The field names iterator.
     */
    private static void iterateOverAllUnvisitedFields(JsonNode local, JsonNode cloud, JsonNode base,
                                                      DataOwner owner, JsonNode result,
                                                      Set<String> visited, Iterator<String> fields) {
        while (fields.hasNext()) {
            final String field = fields.next();
            if (visited.contains(field)) {
                continue;
            }
            final JsonNode localValue = local.get(field);
            final JsonNode cloudValue = cloud.get(field);
            final JsonNode baseValue = base.get(field);
            JsonNode mergedResult = getMergedNode(localValue, cloudValue, baseValue, owner);
            ((ObjectNode) result).set(field, mergedResult);
            if (mergedResult == null) {
                visited.add(field);
            }
            //visited.add(field);
        }
    }

    /**
     * Resolves the conflict based on the owner.
     *
     * @param local The current local document.
     * @param cloud The current cloud document.
     * @param owner The data owner which is used during conflict resolution.
     * @return the resolved JSON node to use.
     */
    private static JsonNode resolveConflict(JsonNode local, JsonNode cloud, DataOwner owner) {
        return chooseOwnerValue(local, cloud, owner);
    }

    /**
     * Chooses the correct JSON node based on the owner.
     *
     * @param local The current local document.
     * @param cloud The current cloud document.
     * @param owner The data owner which is used during conflict resolution.
     * @return the owner JSON node.
     */
    private static JsonNode chooseOwnerValue(JsonNode local, JsonNode cloud, DataOwner owner) {
        switch (owner) {
            case LOCAL:
                return local;
            case CLOUD:
                return cloud;
            default:
                throw new IllegalArgumentException("Unsupported owner");
        }
    }
}
