/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync.strategy.model;

import com.aws.greengrass.util.Coerce;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.Map;

@Getter
@AllArgsConstructor
@NoArgsConstructor
@Builder(toBuilder = true)
@Data
public class Strategy {
    /**
     * Type of sync strategy.
     */
    private StrategyType type;

    /**
     * The delay to be used in the sync strategy between multiple sync loop runs.
     */
    private long delay;

    /**
     * The default strategy to be used. Currently it will be set to real time syncing of shadows.
     */
    public static final Strategy DEFAULT_STRATEGY = new Strategy(StrategyType.REALTIME, 0);

    /**
     * Gets the Sync Strategy based on the POJO object.
     *
     * @param pojo the key-value POJO object.
     * @return the converted sync strategy object.
     */
    public static Strategy fromPojo(Map<String, Object> pojo) {
        StrategyBuilder strategy = Strategy.builder();
        for (Map.Entry<String, Object> entry : pojo.entrySet()) {
            switch (entry.getKey()) {
                case "type":
                    strategy.type(StrategyType.fromCode(Coerce.toString(entry.getValue())));
                    break;
                case "delay":
                    strategy.delay(Coerce.toLong(entry.getValue()));
                    break;
                default:
                    break;
            }
        }
        return strategy.build();
    }
}
