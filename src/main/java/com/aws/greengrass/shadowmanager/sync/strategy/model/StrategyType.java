/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync.strategy.model;

import com.aws.greengrass.util.Utils;

public enum StrategyType {
    REALTIME("realTime"),
    PERIODIC("periodic");

    /**
     * Code for the sync strategy type which will be used in the configuration.
     */
    String code;

    StrategyType(String code) {
        this.code = code;
    }

    /**
     * Gets the strategy type enum based on the code.
     *
     * @param code the code for the sync strategy type.
     * @return the sync strategy type enum for the code.
     */
    public static StrategyType fromCode(String code) {
        if (Utils.isEmpty(code)) {
            return REALTIME;
        }
        switch (code) {
            case "periodic":
                return PERIODIC;
            case "realTime":
            default:
                return REALTIME;
        }
    }
}
