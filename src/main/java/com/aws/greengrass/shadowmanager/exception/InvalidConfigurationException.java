/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.exception;

public class InvalidConfigurationException extends RuntimeException {
    private static final long serialVersionUID = -1488980916089225328L;

    public InvalidConfigurationException(final String message) {
        super(message);
    }

    public InvalidConfigurationException(final Throwable ex) {
        super(ex);
    }
}
