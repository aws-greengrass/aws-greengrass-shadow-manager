/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.exception;

public class ShadowManagerException extends RuntimeException {

    public ShadowManagerException(String err) {
        super(err);
    }

    public ShadowManagerException(Exception err) {
        super(err);
    }
}
