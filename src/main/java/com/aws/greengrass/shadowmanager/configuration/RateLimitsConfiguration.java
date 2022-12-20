/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.configuration;

import com.aws.greengrass.config.Topics;
import com.aws.greengrass.shadowmanager.util.Validator;
import com.aws.greengrass.util.Coerce;
import lombok.Getter;

import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_MAX_LOCAL_REQUESTS_RATE_PER_THING_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_MAX_OUTBOUND_UPDATES_PS_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_MAX_TOTAL_LOCAL_REQUESTS_RATE;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_RATE_LIMITS_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.DEFAULT_LOCAL_REQUESTS_RATE;
import static com.aws.greengrass.shadowmanager.model.Constants.DEFAULT_MAX_OUTBOUND_SYNC_UPDATES_PS;
import static com.aws.greengrass.shadowmanager.model.Constants.DEFAULT_TOTAL_LOCAL_REQUESTS_RATE;

public final class RateLimitsConfiguration {
    @Getter
    private final int maxTotalLocalRequestRate;
    @Getter
    private final int maxLocalRequestRatePerThing;
    @Getter
    private final int maxOutboundUpdatesPerSecond;

    private static int getMaxTotalLocalRequestRateFromTopics(Topics rateLimitsTopics) {
        int maxTotalLocalRequestRate = Coerce.toInt(rateLimitsTopics
                .findOrDefault(DEFAULT_TOTAL_LOCAL_REQUESTS_RATE, CONFIGURATION_MAX_TOTAL_LOCAL_REQUESTS_RATE));
        Validator.validateTotalLocalRequestRate(maxTotalLocalRequestRate);
        return maxTotalLocalRequestRate;
    }

    private static int getMaxLocalRequestRatePerThingFromTopics(Topics rateLimitsTopics) {
        int maxLocalRequestRatePerThing = Coerce.toInt(rateLimitsTopics
                .findOrDefault(DEFAULT_LOCAL_REQUESTS_RATE, CONFIGURATION_MAX_LOCAL_REQUESTS_RATE_PER_THING_TOPIC));
        Validator.validateLocalShadowRequestsPerThingPerSecond(maxLocalRequestRatePerThing);
        return maxLocalRequestRatePerThing;
    }

    private static int getMaxOutboundUpdatesPerSecondFromTopics(Topics rateLimitsTopics) {
        int maxOutboundUpdatesPerSecond = Coerce.toInt(rateLimitsTopics
                .findOrDefault(DEFAULT_MAX_OUTBOUND_SYNC_UPDATES_PS, CONFIGURATION_MAX_OUTBOUND_UPDATES_PS_TOPIC));
        Validator.validateOutboundSyncUpdatesPerSecond(maxOutboundUpdatesPerSecond);
        return maxOutboundUpdatesPerSecond;
    }

    private RateLimitsConfiguration(int maxLocalRequestRatePerThing,
                                    int maxTotalLocalRequestRate,
                                    int maxOutboundUpdatesPerSecond) {
        this.maxLocalRequestRatePerThing = maxLocalRequestRatePerThing;
        this.maxTotalLocalRequestRate = maxTotalLocalRequestRate;
        this.maxOutboundUpdatesPerSecond = maxOutboundUpdatesPerSecond;
    }

    private static RateLimitsConfiguration getRateLimitsConfigurationFromTopics(Topics topics) {
        Topics rateLimitsTopics = topics.lookupTopics(CONFIGURATION_RATE_LIMITS_TOPIC);
        int maxTotalLocalRequestRate = getMaxTotalLocalRequestRateFromTopics(rateLimitsTopics);
        int maxLocalRequestRatePerThing = getMaxLocalRequestRatePerThingFromTopics(rateLimitsTopics);
        int maxOutboundUpdatesPerSecond = getMaxOutboundUpdatesPerSecondFromTopics(rateLimitsTopics);

        return new RateLimitsConfiguration(maxLocalRequestRatePerThing, maxTotalLocalRequestRate,
                maxOutboundUpdatesPerSecond);
    }

    /**
     * Creates a new rate limits configuration object and triggers updates based on previous configuration.
     *
     * @param serviceTopics    current configuration topics
     * @return rate limits configuration objects
     */
    public static RateLimitsConfiguration from(Topics serviceTopics) {
        return getRateLimitsConfigurationFromTopics(serviceTopics);
    }

}
