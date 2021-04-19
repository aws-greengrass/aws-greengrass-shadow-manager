/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.exception;

/**
 * An exception indicating the sync request needs to be skipped.
 */
public class SkipSyncRequestException extends Exception {
    private static final long serialVersionUID = -1488980916089225328L;

    public SkipSyncRequestException(String message) {
        super(message);
    }

    public SkipSyncRequestException(String message, Throwable cause) {
        super(message, cause);
    }
}
