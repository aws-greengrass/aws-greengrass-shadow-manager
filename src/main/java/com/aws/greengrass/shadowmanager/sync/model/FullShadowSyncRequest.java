/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync.model;

import com.aws.greengrass.shadowmanager.ShadowManagerDAO;
import com.aws.greengrass.shadowmanager.exception.SyncException;
import com.aws.greengrass.shadowmanager.ipc.DeleteThingShadowIPCHandler;
import com.aws.greengrass.shadowmanager.ipc.UpdateThingShadowIPCHandler;
import lombok.NonNull;

/**
 * Sync request handling a full sync request for a particular shadow.
 */
public class FullShadowSyncRequest extends BaseSyncRequest {

    @NonNull
    UpdateThingShadowIPCHandler updateThingShadowIPCHandler;

    @NonNull
    DeleteThingShadowIPCHandler deleteThingShadowIPCHandler;

    /**
     * Ctr for FullShadowSyncRequest.
     *
     * @param thingName                   The thing name associated with the sync shadow update
     * @param shadowName                  The shadow name associated with the sync shadow update
     * @param dao                         Local shadow database management
     * @param deleteThingShadowIPCHandler Reference to the DeleteThingShadow IPC Handler
     * @param updateThingShadowIPCHandler Reference to the UpdateThingShadow IPC Handler
     */
    public FullShadowSyncRequest(String thingName,
                                 String shadowName,
                                 ShadowManagerDAO dao,
                                 UpdateThingShadowIPCHandler updateThingShadowIPCHandler,
                                 DeleteThingShadowIPCHandler deleteThingShadowIPCHandler) {
        super(thingName, shadowName, dao);
        this.updateThingShadowIPCHandler = updateThingShadowIPCHandler;
        this.deleteThingShadowIPCHandler = deleteThingShadowIPCHandler;
    }

    @Override
    public void execute() throws SyncException {

    }
}
