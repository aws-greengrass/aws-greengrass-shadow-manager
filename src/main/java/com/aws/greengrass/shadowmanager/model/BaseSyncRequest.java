/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.model;

import lombok.NonNull;

/**
 * Base class for all sync requests
 */
public class BaseSyncRequest extends ShadowRequest {

    @NonNull
    int version;

    // TODO: determine update time format
    @NonNull
    int updateTime;


    /**
     * Ctr for BaseSyncRequest.
     */
    public BaseSyncRequest(String thingName, String shadowName, int updateTime, int version) {
        super(thingName, shadowName);
        this.version = version;
        this.updateTime = updateTime;
    }
}
