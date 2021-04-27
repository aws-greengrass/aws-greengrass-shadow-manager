/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync.model;

import com.aws.greengrass.shadowmanager.model.ShadowRequest;

/**
 * Base class for all sync requests.
 */
public abstract class BaseSyncRequest extends ShadowRequest implements SyncRequest {

    /**
     * Ctr for BaseSyncRequest.
     *
     * @param thingName  The thing name associated with the sync shadow update
     * @param shadowName The shadow name associated with the sync shadow update
     */
    public BaseSyncRequest(String thingName,
                           String shadowName) {
        super(thingName, shadowName);
    }
}
