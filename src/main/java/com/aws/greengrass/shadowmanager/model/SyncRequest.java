/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.aws.greengrass.shadowmanager.model;

/**
 * Interface for a sync request.
 */
public interface SyncRequest {

    /**
     * Executes sync request.
     */
    public void executeSync();
}
