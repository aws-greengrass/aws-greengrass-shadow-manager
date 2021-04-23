/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.exception;

/**
 * Exception indicating that expected shadow was not found.
 */
public class UnknownShadowException extends RuntimeException {
    private static final long serialVersionUID = -1488980916089225328L;

    public UnknownShadowException(final String message) {
        super(message);
    }
}
