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

    //TODO: remove this warning.
    @SuppressWarnings("PMD.UnusedPrivateField")
    private final Instant lastExceptionTime;

    public RetryableException(Throwable e) {
        super(e);
        this.lastExceptionTime = Instant.now();
    }
}
