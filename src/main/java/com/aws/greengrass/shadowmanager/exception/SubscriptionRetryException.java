/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.exception;

public class SubscriptionRetryException extends Exception {
    private static final long serialVersionUID = -1488980916089225328L;

    public SubscriptionRetryException(final String message) {
        super(message);
    }
}
