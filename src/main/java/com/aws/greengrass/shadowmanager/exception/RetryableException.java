/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.exception;

import java.time.Instant;

/**
 * An exception indicating the sync request needs to be retried.
 */
public class RetryableException extends Exception {
    private static final long serialVersionUID = -1488980916089225328L;

    private final Instant lastExceptionTime;
    private final int httpStatusCode;

    public RetryableException(String message, int httpStatusCode) {
        super(message);
        this.lastExceptionTime = Instant.now();
        this.httpStatusCode = httpStatusCode;
    }
}
