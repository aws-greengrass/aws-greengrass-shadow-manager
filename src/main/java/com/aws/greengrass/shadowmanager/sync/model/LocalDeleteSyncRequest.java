/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync.model;

import com.aws.greengrass.shadowmanager.ShadowManagerDAO;
import com.aws.greengrass.shadowmanager.exception.SyncException;
import com.aws.greengrass.shadowmanager.ipc.DeleteThingShadowIPCHandler;
import lombok.NonNull;

/**
 * Sync request to delete a locally stored shadow.
 */
public class LocalDeleteSyncRequest extends BaseSyncRequest {

    @NonNull
    DeleteThingShadowIPCHandler deleteThingShadowIPCHandler;

    /**
     * Ctr for LocalDeleteSyncRequest.
     *
     * @param thingName                   The thing name associated with the sync shadow update
     * @param shadowName                  The shadow name associated with the sync shadow update
     * @param dao                         Local shadow database management
     * @param deleteThingShadowIPCHandler Reference to the DeleteThingShadow IPC Handler
     */
    public LocalDeleteSyncRequest(String thingName,
                                  String shadowName,
                                  ShadowManagerDAO dao,
                                  DeleteThingShadowIPCHandler deleteThingShadowIPCHandler) {
        super(thingName, shadowName, dao);
        this.deleteThingShadowIPCHandler = deleteThingShadowIPCHandler;
    }

    @Override
    public void execute() throws SyncException {

    }
}
