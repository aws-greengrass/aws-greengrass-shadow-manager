/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.configuration;

import com.aws.greengrass.config.Topics;
import com.aws.greengrass.dependency.Context;
import com.aws.greengrass.shadowmanager.exception.InvalidConfigurationException;
import com.aws.greengrass.shadowmanager.ipc.InboundRateLimiter;
import com.aws.greengrass.shadowmanager.sync.IotDataPlaneClientWrapper;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.testcommons.testutilities.GGServiceTestUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;

import static com.aws.greengrass.componentmanager.KernelConfigResolver.CONFIGURATION_CONFIG_KEY;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_MAX_LOCAL_REQUESTS_RATE_PER_THING_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_MAX_OUTBOUND_UPDATES_PS_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_MAX_TOTAL_LOCAL_REQUESTS_RATE;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_RATE_LIMITS_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.DEFAULT_LOCAL_REQUESTS_RATE;
import static com.aws.greengrass.shadowmanager.model.Constants.DEFAULT_MAX_OUTBOUND_SYNC_UPDATES_PS;
import static com.aws.greengrass.shadowmanager.model.Constants.DEFAULT_TOTAL_LOCAL_REQUESTS_RATE;
import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith({MockitoExtension.class, GGExtension.class})
class RateLimitConfigurationTest extends GGServiceTestUtil {
    private Topics configurationTopics;
    private final static RateLimitsConfiguration oldConfiguration = null;
    private final static int RATE_LIMIT = 500;
    @Mock
    private InboundRateLimiter mockInboundRateLimiter;
    @Mock
    private IotDataPlaneClientWrapper mockIotDataPlaneClientWrapper;
    private Context context;

    @BeforeEach
    void beforeEach() {
        context = new Context();
        configurationTopics = Topics.of(context, CONFIGURATION_CONFIG_KEY, null);
        context.put(InboundRateLimiter.class, mockInboundRateLimiter);
        context.put(IotDataPlaneClientWrapper.class, mockIotDataPlaneClientWrapper);
    }

    @AfterEach
    void afterEach() throws IOException {
        context.close();
    }


    @Test
    public void GIVEN_default_configuration_WHEN_getMaxLocalRequestRatePerThing_THEN_return_default() {
        RateLimitsConfiguration rateLimitsConfiguration = RateLimitsConfiguration.from(oldConfiguration, configurationTopics);
        assertThat(rateLimitsConfiguration.getMaxLocalRequestRatePerThing(), is(DEFAULT_LOCAL_REQUESTS_RATE));
        verify(mockInboundRateLimiter, times(1)).setRate(DEFAULT_LOCAL_REQUESTS_RATE);
    }

    @Test
    public void GIVEN_default_configuration_WHEN_getMaxTotalLocalRequestRate_THEN_return_default() {
        RateLimitsConfiguration rateLimitsConfiguration = RateLimitsConfiguration.from(oldConfiguration, configurationTopics);
        assertThat(rateLimitsConfiguration.getMaxTotalLocalRequestRate(), is(DEFAULT_TOTAL_LOCAL_REQUESTS_RATE));
        verify(mockInboundRateLimiter, times(1)).setTotalRate(DEFAULT_TOTAL_LOCAL_REQUESTS_RATE);
    }

    @Test
    public void GIVEN_default_configuration_WHEN_getMaxOutboundUpdatesPerSecond_THEN_return_default() {
        RateLimitsConfiguration rateLimitsConfiguration = RateLimitsConfiguration.from(oldConfiguration, configurationTopics);
        assertThat(rateLimitsConfiguration.getMaxOutboundUpdatesPerSecond(), is(DEFAULT_MAX_OUTBOUND_SYNC_UPDATES_PS));
        verify(mockIotDataPlaneClientWrapper, times(1)).setRate(DEFAULT_MAX_OUTBOUND_SYNC_UPDATES_PS);
    }

    @Test
    public void GIVEN_good_maxLocalRequestRatePerThing_WHEN_getMaxLocalRequestRatePerThing_THEN_return_from_config() {
        configurationTopics.lookup(CONFIGURATION_RATE_LIMITS_TOPIC,
                CONFIGURATION_MAX_LOCAL_REQUESTS_RATE_PER_THING_TOPIC).withValue(RATE_LIMIT);
        RateLimitsConfiguration rateLimitsConfiguration = RateLimitsConfiguration.from(oldConfiguration, configurationTopics);
        assertThat(rateLimitsConfiguration.getMaxLocalRequestRatePerThing(), is(RATE_LIMIT));
        verify(mockInboundRateLimiter, times(1)).setRate(RATE_LIMIT);
    }

    @Test
    public void GIVEN_bad_getMaxLocalRequestRatePerThing_WHEN_getMaxLocalRequestRatePerThing_THEN_return_from_config(ExtensionContext extensionContext) {
        ignoreExceptionOfType(extensionContext, InvalidConfigurationException.class);
        configurationTopics.lookup(CONFIGURATION_RATE_LIMITS_TOPIC,
                CONFIGURATION_MAX_LOCAL_REQUESTS_RATE_PER_THING_TOPIC).withValue(-1);
        assertThrows(InvalidConfigurationException.class, () -> RateLimitsConfiguration.from(oldConfiguration, configurationTopics));
        verify(mockInboundRateLimiter, times(0)).setRate(RATE_LIMIT);
    }

    @Test
    public void GIVEN_good_maxTotalLocalRequestRate_WHEN_getMaxTotalLocalRequestRate_THEN_return_from_config() {
        configurationTopics.lookup(CONFIGURATION_RATE_LIMITS_TOPIC,
                CONFIGURATION_MAX_TOTAL_LOCAL_REQUESTS_RATE).withValue(RATE_LIMIT);
        RateLimitsConfiguration rateLimitsConfiguration = RateLimitsConfiguration.from(oldConfiguration, configurationTopics);
        assertThat(rateLimitsConfiguration.getMaxTotalLocalRequestRate(), is(RATE_LIMIT));
        verify(mockInboundRateLimiter, times(1)).setTotalRate(RATE_LIMIT);
    }

    @Test
    public void GIVEN_bad_maxTotalLocalRequestRate_WHEN_getMaxTotalLocalRequestRate_THEN_return_from_config(ExtensionContext extensionContext) {
        ignoreExceptionOfType(extensionContext, InvalidConfigurationException.class);
        configurationTopics.lookup(CONFIGURATION_RATE_LIMITS_TOPIC,
                CONFIGURATION_MAX_TOTAL_LOCAL_REQUESTS_RATE).withValue(-1);
        assertThrows(InvalidConfigurationException.class, () -> RateLimitsConfiguration.from(oldConfiguration, configurationTopics));
        verify(mockInboundRateLimiter, times(0)).setTotalRate(RATE_LIMIT);
    }

    @Test
    public void GIVEN_good_maxOutboundUpdatesPS_WHEN_getMaxOutboundUpdatesPerSecond_THEN_return_from_config() {
        configurationTopics.lookup(CONFIGURATION_RATE_LIMITS_TOPIC,
                CONFIGURATION_MAX_OUTBOUND_UPDATES_PS_TOPIC).withValue(RATE_LIMIT);
        RateLimitsConfiguration rateLimitsConfiguration = RateLimitsConfiguration.from(oldConfiguration, configurationTopics);
        assertThat(rateLimitsConfiguration.getMaxOutboundUpdatesPerSecond(), is(RATE_LIMIT));
        verify(mockIotDataPlaneClientWrapper, times(1)).setRate(RATE_LIMIT);
    }

    @Test
    public void GIVEN_bad_maxLocalRequestRatePerThing_WHEN_getMaxLocalRequestRatePerThing_THEN_return_from_config(ExtensionContext extensionContext) {
        ignoreExceptionOfType(extensionContext, InvalidConfigurationException.class);
        configurationTopics.lookup(CONFIGURATION_RATE_LIMITS_TOPIC,
                CONFIGURATION_MAX_OUTBOUND_UPDATES_PS_TOPIC).withValue(-1);
        assertThrows(InvalidConfigurationException.class, () -> RateLimitsConfiguration.from(oldConfiguration, configurationTopics));
        verify(mockIotDataPlaneClientWrapper, times(0)).setRate(RATE_LIMIT);
    }
}

