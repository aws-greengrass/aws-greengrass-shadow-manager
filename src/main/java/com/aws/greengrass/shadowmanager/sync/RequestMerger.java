/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync;

import com.aws.greengrass.shadowmanager.ShadowManagerDAO;
import com.aws.greengrass.shadowmanager.ipc.DeleteThingShadowRequestHandler;
import com.aws.greengrass.shadowmanager.ipc.UpdateThingShadowRequestHandler;
import com.aws.greengrass.shadowmanager.sync.model.CloudDeleteSyncRequest;
import com.aws.greengrass.shadowmanager.sync.model.CloudUpdateSyncRequest;
import com.aws.greengrass.shadowmanager.sync.model.FullShadowSyncRequest;
import com.aws.greengrass.shadowmanager.sync.model.LocalDeleteSyncRequest;
import com.aws.greengrass.shadowmanager.sync.model.LocalUpdateSyncRequest;
import com.aws.greengrass.shadowmanager.sync.model.SyncRequest;

import javax.inject.Inject;

/**
 * Merge requests that can be combined together. Falls back to FullSync if requests cannot be combined in a
 * meaningful way.
 */
class RequestMerger {

    private final ShadowManagerDAO dao;
    private final UpdateThingShadowRequestHandler updateHandler;
    private final DeleteThingShadowRequestHandler deleteHandler;

    /**
     * Construct a new instance.
     *
     * @param dao a data access object
     * @param updateHandler an update handler
     * @param deleteHandler a delete handler
     */
    @Inject
    public RequestMerger(ShadowManagerDAO dao, UpdateThingShadowRequestHandler updateHandler,
            DeleteThingShadowRequestHandler deleteHandler) {
        this.dao = dao;
        this.updateHandler = updateHandler;
        this.deleteHandler = deleteHandler;
    }

    /**
     * Merge two requests into one. This implementation returns a {@link FullShadowSyncRequest}.
     *
     * @param oldValue a request to merge.
     * @param value a request to merge.
     * @return a merged request
     */
    @SuppressWarnings({"PMD.EmptyIfStmt"})
    public SyncRequest merge(SyncRequest oldValue, SyncRequest value) {
        if (oldValue instanceof FullShadowSyncRequest) {
            return oldValue;
        }
        if (value instanceof FullShadowSyncRequest) {
            return value;
        }

        if (oldValue instanceof CloudUpdateSyncRequest && value instanceof CloudUpdateSyncRequest) {
            // TODO: combine existing requests
        } else if (oldValue instanceof LocalUpdateSyncRequest && value instanceof LocalUpdateSyncRequest) {
            // TODO: combine existing requests
        } else if (oldValue instanceof CloudUpdateSyncRequest && value instanceof CloudDeleteSyncRequest
            || oldValue instanceof LocalUpdateSyncRequest && value instanceof LocalDeleteSyncRequest) {
            // update followed by delete, just send the delete
            return value;
        } else if (oldValue instanceof CloudDeleteSyncRequest && value instanceof CloudDeleteSyncRequest
            || oldValue instanceof LocalDeleteSyncRequest && value instanceof LocalDeleteSyncRequest) {
            // this should never happen (multiple local or multiple cloud deletes) but it can safely return either value
            return oldValue;
        } else if (oldValue instanceof CloudDeleteSyncRequest && value instanceof LocalDeleteSyncRequest
            || oldValue instanceof LocalDeleteSyncRequest && value instanceof CloudDeleteSyncRequest) {
            // TODO: support simultaneous delete without full sync
        } else if (oldValue instanceof CloudUpdateSyncRequest && value instanceof LocalUpdateSyncRequest
                || oldValue instanceof LocalUpdateSyncRequest && value instanceof CloudUpdateSyncRequest) {
            // TODO: merge bi-directional updates like this without full sync - this is almost
            // the same as a full sync but with partial updates
        }


        // Instead of a partial update, a full sync request will force a get of the latest local and remote shadows
        return new FullShadowSyncRequest(value.getThingName(), value.getShadowName(),
                dao, updateHandler, deleteHandler);
    }
}
