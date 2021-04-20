/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.exception;

public class SyncException extends RuntimeException {
    private static final long serialVersionUID = -1488980916089225328L;

    public SyncException(final String message) {
        super(message);
    }

    public SyncException(final Throwable ex) {
        super(ex);
    }
}