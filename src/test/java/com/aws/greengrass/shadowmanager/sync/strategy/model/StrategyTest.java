/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync.strategy.model;

import com.aws.greengrass.shadowmanager.exception.InvalidConfigurationException;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.Map;

import static com.aws.greengrass.shadowmanager.model.Constants.STRATEGY_TYPE_PERIODIC;
import static com.aws.greengrass.shadowmanager.model.Constants.STRATEGY_TYPE_REAL_TIME;
import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith({MockitoExtension.class, GGExtension.class})
class StrategyTest {

    @Test
    void GIVEN_good_realTime_pojo_WHEN_fromPojo_THEN_gets_the_correct_strategy_object() {
        Map<String, Object> pojo = new HashMap<>();
        pojo.put("type", STRATEGY_TYPE_REAL_TIME);
        Strategy strategy = Strategy.fromPojo(pojo);
        assertThat(strategy.getType(), is(StrategyType.REALTIME));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "badType"})
    void GIVEN_bad_value_pojo_WHEN_fromPojo_THEN_throws_InvalidConfigurationException(String type, ExtensionContext extensionContext) {
        ignoreExceptionOfType(extensionContext, InvalidConfigurationException.class);
        Map<String, Object> pojo = new HashMap<>();
        pojo.put("type", type);
        assertThrows(InvalidConfigurationException.class, () -> Strategy.fromPojo(pojo));
    }

    @Test
    void GIVEN_periodic_pojo_WHEN_fromPojo_THEN_gets_the_correct_strategy_object() {
        Map<String, Object> pojo = new HashMap<>();
        pojo.put("type", STRATEGY_TYPE_PERIODIC);
        pojo.put("delay", "100");
        Strategy strategy = Strategy.fromPojo(pojo);
        assertThat(strategy.getType(), is(StrategyType.PERIODIC));
        assertThat(strategy.getDelay(), is(100L));
    }
}
