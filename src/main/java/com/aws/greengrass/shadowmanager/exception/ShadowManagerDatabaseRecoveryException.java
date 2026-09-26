/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.exception;

/**
 * An exception indicating the local shadow database could not be recovered (reopened or recreated).
 */
public class ShadowManagerDatabaseRecoveryException extends Exception {
    private static final long serialVersionUID = -4512983447715286217L;

    public ShadowManagerDatabaseRecoveryException(Throwable e) {
        super(e);
    }
}
