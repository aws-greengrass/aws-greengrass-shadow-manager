/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync.model;

import com.aws.greengrass.shadowmanager.ShadowManagerDAO;
import com.aws.greengrass.shadowmanager.exception.SyncException;
import com.aws.greengrass.shadowmanager.ipc.UpdateThingShadowIPCHandler;
import lombok.NonNull;

/**
 * Sync request to update locally stored shadow.
 */
public class LocalUpdateSyncRequest extends BaseSyncRequest {

    @NonNull
    UpdateThingShadowIPCHandler updateThingShadowIPCHandler;

    /**
     * Ctr for LocalUpdateSyncRequest.
     *
     * @param thingName                   The thing name associated with the sync shadow update
     * @param shadowName                  The shadow name associated with the sync shadow update
     * @param dao                         Local shadow database management
     * @param updateThingShadowIPCHandler Reference to the UpdateThingShadow IPC Handler
     */
    public LocalUpdateSyncRequest(String thingName,
                                  String shadowName,
                                  ShadowManagerDAO dao,
                                  UpdateThingShadowIPCHandler updateThingShadowIPCHandler) {
        super(thingName, shadowName, dao);
        this.updateThingShadowIPCHandler = updateThingShadowIPCHandler;
    }

    @Override
    public void execute() throws SyncException {

    }
}
