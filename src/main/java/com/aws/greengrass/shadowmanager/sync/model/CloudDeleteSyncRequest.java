/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync.model;

import com.aws.greengrass.shadowmanager.ShadowManagerDAO;
import com.aws.greengrass.shadowmanager.exception.SyncException;
import com.aws.greengrass.shadowmanager.ipc.DeleteThingShadowIPCHandler;
import lombok.NonNull;

import java.time.Instant;

/**
 * Sync request handling a delete from cloud.
 */
public class CloudDeleteSyncRequest extends BaseSyncRequest {

    @NonNull
    DeleteThingShadowIPCHandler deleteThingShadowIPCHandler;

    /**
     * Ctr for CloudDeleteSyncRequest.
     *
     * @param thingName                   The thing name associated with the sync shadow update
     * @param shadowName                  The shadow name associated with the sync shadow update
     * @param updateTime                  The update time of the specific sync shadow update
     * @param version                     The version of the specific sync shadow update
     * @param dao                         Local shadow database management
     * @param deleteThingShadowIPCHandler Reference to the DeleteThingShadow IPC Handler
     */
    public CloudDeleteSyncRequest(String thingName,
                                  String shadowName,
                                  Instant updateTime,
                                  int version,
                                  ShadowManagerDAO dao,
                                  DeleteThingShadowIPCHandler deleteThingShadowIPCHandler) {
        super(thingName, shadowName, updateTime, version, dao);
        this.deleteThingShadowIPCHandler = deleteThingShadowIPCHandler;
    }

    @Override
    public void execute() throws SyncException {

    }
}
