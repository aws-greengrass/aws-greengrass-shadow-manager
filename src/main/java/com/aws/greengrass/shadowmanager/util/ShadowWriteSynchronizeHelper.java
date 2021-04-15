/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.util;

import com.aws.greengrass.shadowmanager.model.ShadowRequest;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Class to handle synchronization objects for write operations for a local shadow.
 */
public class ShadowWriteSynchronizeHelper {
    private final Map<String, Object> thingLocksMap = new ConcurrentHashMap<>();

    /**
     * Gets the static lock object for a thing's shadow which will be used to synchronize the operations being
     * performed on a particular shadow.
     *
     * @param shadowRequest  The thing name.
     * @return the static lock object for a thing's shadow.
     */
    public synchronized Object getThingShadowLock(ShadowRequest shadowRequest) {
        thingLocksMap.computeIfAbsent(shadowRequest.computeShadowLockKey(), key -> new ConcurrentHashMap<>());
        return thingLocksMap.get(shadowRequest.computeShadowLockKey());
    }
}
