/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync.model;

/**
 * Enum to indicate the shadow sync directionality.
 */
public enum Direction {
    BIDIRECTIONAL,
    FROMDEVICEONLY,
    FROMCLOUDONLY
}
