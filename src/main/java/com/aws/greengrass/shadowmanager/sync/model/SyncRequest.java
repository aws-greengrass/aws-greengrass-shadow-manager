/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync.model;

import com.aws.greengrass.shadowmanager.exception.RetryableException;
import com.aws.greengrass.shadowmanager.exception.SkipSyncRequestException;
import com.aws.greengrass.shadowmanager.exception.SyncException;

/**
 * Interface for a sync request.
 */
public interface SyncRequest {

    String getThingName();

    String getShadowName();

    /**
     * Executes sync request.
     *
     * @throws SyncException            When error occurs in sync operation.
     * @throws RetryableException       When error occurs in sync operation indicating a request needs to be retried
     * @throws SkipSyncRequestException When error occurs in sync operation indicating a request needs to be skipped.
     */
    void execute() throws SyncException, RetryableException, SkipSyncRequestException;
}
