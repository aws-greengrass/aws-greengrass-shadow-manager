/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync.model;

import com.aws.greengrass.shadowmanager.exception.RetryableException;
import com.aws.greengrass.shadowmanager.exception.SkipSyncRequestException;
import com.aws.greengrass.shadowmanager.exception.SyncException;
import software.amazon.awssdk.aws.greengrass.model.ConflictError;

/**
 * Interface for a sync request.
 */
public interface SyncRequest {

    /**
     * Executes sync request.
     *
     * @throws SyncException When error occurs in sync operation
     */
    void execute() throws SyncException, RetryableException, SkipSyncRequestException, ConflictError;
}
