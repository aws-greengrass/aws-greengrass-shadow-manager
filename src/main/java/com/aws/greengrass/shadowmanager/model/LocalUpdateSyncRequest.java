/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.model;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Sync request handling an update from local shadow update
 */
public class LocalUpdateSyncRequest extends BaseSyncRequest implements SyncRequest {

    // TODO: determine correct type
    JsonNode updateDocument;


    /**
     * Ctr for LocalUpdateSyncRequest.
     */
    public LocalUpdateSyncRequest(String thingName, String shadowName, int updateTime, int version, JsonNode updateDocument) {
        super(thingName, shadowName, updateTime, version);
        this.updateDocument = updateDocument;
    }

    @Override
    public void executeSync() {

    }
}
