/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync.model;

import com.aws.greengrass.shadowmanager.ShadowManagerDAO;
import com.aws.greengrass.shadowmanager.exception.SyncException;
import com.aws.greengrass.shadowmanager.ipc.DeleteThingShadowRequestHandler;
import com.aws.greengrass.shadowmanager.ipc.UpdateThingShadowRequestHandler;
import lombok.NonNull;

/**
 * Sync request handling a full sync request for a particular shadow.
 */
public class FullShadowSyncRequest extends BaseSyncRequest {

    @NonNull
    UpdateThingShadowRequestHandler updateThingShadowRequestHandler;

    @NonNull
    DeleteThingShadowRequestHandler deleteThingShadowRequestHandler;

    /**
     * Ctr for FullShadowSyncRequest.
     *
     * @param thingName                   The thing name associated with the sync shadow update
     * @param shadowName                  The shadow name associated with the sync shadow update
     * @param dao                         Local shadow database management
     * @param deleteThingShadowRequestHandler Reference to the DeleteThingShadow IPC Handler
     * @param updateThingShadowRequestHandler Reference to the UpdateThingShadow IPC Handler
     */
    public FullShadowSyncRequest(String thingName,
                                 String shadowName,
                                 ShadowManagerDAO dao,
                                 UpdateThingShadowRequestHandler updateThingShadowRequestHandler,
                                 DeleteThingShadowRequestHandler deleteThingShadowRequestHandler) {
        super(thingName, shadowName, dao);
        this.updateThingShadowRequestHandler = updateThingShadowRequestHandler;
        this.deleteThingShadowRequestHandler = deleteThingShadowRequestHandler;
    }

    @Override
    public void execute() throws SyncException {

    }
}
