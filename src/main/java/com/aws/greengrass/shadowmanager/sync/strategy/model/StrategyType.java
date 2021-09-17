/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync.strategy.model;

import com.aws.greengrass.shadowmanager.exception.InvalidConfigurationException;

import static com.aws.greengrass.shadowmanager.model.Constants.STRATEGY_TYPE_PERIODIC;
import static com.aws.greengrass.shadowmanager.model.Constants.STRATEGY_TYPE_REAL_TIME;

public enum StrategyType {
    REALTIME(STRATEGY_TYPE_REAL_TIME),
    PERIODIC(STRATEGY_TYPE_PERIODIC);

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
     * @throws InvalidConfigurationException if the strategy type is bad.
     */
    public static StrategyType fromCode(String code) throws InvalidConfigurationException {
        if (code == null) {
            throw new InvalidConfigurationException("Unexpected value null for strategy type configuration");
        }
        switch (code) {
            case STRATEGY_TYPE_PERIODIC:
                return PERIODIC;
            case STRATEGY_TYPE_REAL_TIME:
                return REALTIME;
            default:
                throw new InvalidConfigurationException(
                        String.format("Unexpected value %s for strategy type configuration", code));
        }
    }
}
