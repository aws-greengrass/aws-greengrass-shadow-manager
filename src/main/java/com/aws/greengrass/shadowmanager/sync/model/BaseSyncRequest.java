/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync.model;

import com.aws.greengrass.shadowmanager.ShadowManagerDAO;
import com.aws.greengrass.shadowmanager.model.ShadowRequest;
import lombok.NonNull;

/**
 * Base class for all sync requests.
 */
public abstract class BaseSyncRequest extends ShadowRequest implements SyncRequest {

    @NonNull
    ShadowManagerDAO dao;

    /**
     * Ctr for BaseSyncRequest.
     *
     * @param thingName  The thing name associated with the sync shadow update
     * @param shadowName The shadow name associated with the sync shadow update
     * @param dao        Local shadow database management
     */
    public BaseSyncRequest(String thingName,
                           String shadowName,
                           ShadowManagerDAO dao) {
        super(thingName, shadowName);
        this.dao = dao;
    }
}
