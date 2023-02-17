/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync.model;

import com.aws.greengrass.shadowmanager.exception.RetryableException;
import com.aws.greengrass.shadowmanager.exception.SkipSyncRequestException;
import com.aws.greengrass.shadowmanager.exception.UnknownShadowException;

/**
 * Interface for a sync request.
 */
public interface SyncRequest {

    String getThingName();

    String getShadowName();

    /**
     * Executes sync request.
     *
     * @param context context object containing useful objects for requests to use when executing.
     * @throws RetryableException       When error occurs in sync operation indicating a request needs to be retried
     * @throws SkipSyncRequestException When error occurs in sync operation indicating a request needs to be skipped.
     * @throws UnknownShadowException   When shadow not found in the sync table.
     * @throws InterruptedException     if the thread is interrupted while syncing shadow with cloud.
     */
    void execute(SyncContext context) throws RetryableException, SkipSyncRequestException,
            UnknownShadowException, InterruptedException;

}
