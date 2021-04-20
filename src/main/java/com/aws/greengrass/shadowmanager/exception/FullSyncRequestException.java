/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.exception;

/**
 * Exception indicating that a full sync is necessary during execution of a sync request.
 */
public class FullSyncRequestException extends Exception {
    private static final long serialVersionUID = -1488980916089225328L;

    public FullSyncRequestException(Throwable e) {
        super(e);
    }

    public FullSyncRequestException(String message) {
        super(message);
    }

}
