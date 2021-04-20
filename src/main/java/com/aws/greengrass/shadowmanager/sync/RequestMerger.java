/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync;

import com.aws.greengrass.shadowmanager.ShadowManagerDAO;
import com.aws.greengrass.shadowmanager.ipc.DeleteThingShadowIPCHandler;
import com.aws.greengrass.shadowmanager.ipc.UpdateThingShadowIPCHandler;
import com.aws.greengrass.shadowmanager.sync.model.FullShadowSyncRequest;
import com.aws.greengrass.shadowmanager.sync.model.SyncRequest;

import javax.inject.Inject;

public class RequestMerger {

    private final ShadowManagerDAO dao;
    private final UpdateThingShadowIPCHandler updateHandler;
    private final DeleteThingShadowIPCHandler deleteHandler;

    /**
     * Construct a new instance.
     * @param dao a data access object
     * @param updateHandler an update handler
     * @param deleteHandler a delete handler
     */
    @Inject
    public RequestMerger(ShadowManagerDAO dao, UpdateThingShadowIPCHandler updateHandler,
            DeleteThingShadowIPCHandler deleteHandler) {
        this.dao = dao;
        this.updateHandler = updateHandler;
        this.deleteHandler = deleteHandler;
    }

    /**
     * Merge two requests into one. This implementation returns a {@link FullShadowSyncRequest}.
     *
     * @param oldValue a request to merge.
     * @param value a requrest to merge.
     * @return a merged request
     */
    public SyncRequest merge(SyncRequest oldValue, SyncRequest value) {
        if (oldValue instanceof FullShadowSyncRequest) {
            return oldValue;
        }
        if (value instanceof FullShadowSyncRequest) {
            return value;
        }
        // Instead of a partial update, a full sync request will force a get of the latest local and remote shadows
        return new FullShadowSyncRequest(value.getThingName(), value.getShadowName(),
                dao, updateHandler, deleteHandler);
    }
}
