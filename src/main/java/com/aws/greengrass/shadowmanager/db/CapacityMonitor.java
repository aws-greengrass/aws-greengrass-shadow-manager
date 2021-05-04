/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.db;


/**
 * Interface for monitoring capacity of a database.
 */
public interface CapacityMonitor {
    /**
     * Answer whether capacity is exceeded.
     *
     * @return true if the capacity is exceeded, otherwise false.
     */
    boolean isCapacityExceeded();
}
