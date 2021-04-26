/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.util;

/**
 * Enum to state whose value should be used in case of a conflict.
 */
public enum DataOwner {
    LOCAL,
    CLOUD
}
