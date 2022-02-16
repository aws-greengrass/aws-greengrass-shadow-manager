/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync.model;

import lombok.Getter;

/**
 * Enum to indicate the shadow sync directionality.
 */
public enum Direction {
    BETWEEN_DEVICE_AND_CLOUD("betweenDeviceAndCloud"),
    DEVICE_TO_CLOUD("deviceToCloud"),
    CLOUD_TO_DEVICE("cloudToDevice");

    /**
     * Code for the sync strategy type which will be used in the configuration.
     */
    @Getter
    private final String code;

    Direction(String code) {
        this.code = code;
    }

    /**
     * Gets the direction enum from a string literal.
     *
     * @param direction the direction string literal.
     * @return the direction enum
     * @throws IllegalArgumentException if string is not a valid enum value.
     */
    public static Direction getDirection(String direction) throws IllegalArgumentException {
        for (Direction dir : values()) {
            if (dir.code.equalsIgnoreCase(direction)) {
                return dir;
            }
        }
        throw new IllegalArgumentException("Unknown direction value.");

    }
}
