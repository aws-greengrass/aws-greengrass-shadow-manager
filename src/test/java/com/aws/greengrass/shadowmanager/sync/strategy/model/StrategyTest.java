/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync.strategy.model;

import com.aws.greengrass.testcommons.testutilities.GGExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@ExtendWith({MockitoExtension.class, GGExtension.class})
class StrategyTest {

    @ParameterizedTest
    @ValueSource(strings = {"realTime", "", "badType"})
    void GIVEN_realTime_pojo_WHEN_fromPojo_THEN_gets_the_correct_strategy_object(String type) {
        Map<String, Object> pojo = new HashMap<>();
        pojo.put("type", type);
        Strategy strategy = Strategy.fromPojo(pojo);
        assertThat(strategy.getType(), is(StrategyType.REALTIME));
    }

    @Test
    void GIVEN_periodic_pojo_WHEN_fromPojo_THEN_gets_the_correct_strategy_object() {
        Map<String, Object> pojo = new HashMap<>();
        pojo.put("type", "periodic");
        pojo.put("delay", "100");
        Strategy strategy = Strategy.fromPojo(pojo);
        assertThat(strategy.getType(), is(StrategyType.PERIODIC));
        assertThat(strategy.getDelay(), is(100L));
    }
}
