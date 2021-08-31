/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync;

import com.aws.greengrass.shadowmanager.sync.model.SyncContext;
import com.aws.greengrass.shadowmanager.sync.model.SyncRequest;
import com.aws.greengrass.util.RetryUtils;

/**
 * Interface for executing sync requests with retries.
 */
@FunctionalInterface
public interface Retryer {
    @SuppressWarnings("PMD.SignatureDeclareThrowsException")
    void run(RetryUtils.RetryConfig config, SyncRequest request, SyncContext context) throws Exception;
}
