/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.configuration;

import com.aws.greengrass.config.Topics;
import com.aws.greengrass.shadowmanager.ipc.InboundRateLimiter;
import com.aws.greengrass.shadowmanager.sync.IotDataPlaneClientWrapper;
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
    private final InboundRateLimiter inboundRateLimiter;
    private final IotDataPlaneClientWrapper iotDataPlaneClientWrapper;

    private static int getMaxTotalLocalRequestRateFromTopics(Topics rateLimitsTopics) {
        int maxTotalLocalRequestRate = Coerce.toInt(rateLimitsTopics
                .lookup(CONFIGURATION_MAX_TOTAL_LOCAL_REQUESTS_RATE)
                .dflt(DEFAULT_TOTAL_LOCAL_REQUESTS_RATE));
        Validator.validateTotalLocalRequestRate(maxTotalLocalRequestRate);
        return maxTotalLocalRequestRate;
    }

    private static int getMaxLocalRequestRatePerThingFromTopics(Topics rateLimitsTopics) {
        int maxLocalRequestRatePerThing = Coerce.toInt(rateLimitsTopics
                .lookup(CONFIGURATION_MAX_LOCAL_REQUESTS_RATE_PER_THING_TOPIC)
                .dflt(DEFAULT_LOCAL_REQUESTS_RATE));
        Validator.validateLocalShadowRequestsPerThingPerSecond(maxLocalRequestRatePerThing);
        return maxLocalRequestRatePerThing;
    }

    private static int getMaxOutboundUpdatesPerSecondFromTopics(Topics rateLimitsTopics) {
        int maxOutboundUpdatesPerSecond = Coerce.toInt(rateLimitsTopics
                .lookup(CONFIGURATION_MAX_OUTBOUND_UPDATES_PS_TOPIC)
                .dflt(DEFAULT_MAX_OUTBOUND_SYNC_UPDATES_PS));
        Validator.validateOutboundSyncUpdatesPerSecond(maxOutboundUpdatesPerSecond);
        return maxOutboundUpdatesPerSecond;
    }

    private RateLimitsConfiguration(int maxLocalRequestRatePerThing,
                                    int maxTotalLocalRequestRate,
                                    int maxOutboundUpdatesPerSecond,
                                    InboundRateLimiter inboundRateLimiter,
                                    IotDataPlaneClientWrapper iotDataPlaneClientWrapper) {

        this.maxLocalRequestRatePerThing = maxLocalRequestRatePerThing;
        this.maxTotalLocalRequestRate = maxTotalLocalRequestRate;
        this.maxOutboundUpdatesPerSecond = maxOutboundUpdatesPerSecond;
        this.inboundRateLimiter = inboundRateLimiter;
        this.iotDataPlaneClientWrapper = iotDataPlaneClientWrapper;
    }

    private static RateLimitsConfiguration getRateLimitsConfigurationFromTopics(Topics topics) {
        Topics rateLimitsTopics = topics.lookupTopics(CONFIGURATION_RATE_LIMITS_TOPIC);
        int maxTotalLocalRequestRate = getMaxTotalLocalRequestRateFromTopics(rateLimitsTopics);
        int maxLocalRequestRatePerThing = getMaxLocalRequestRatePerThingFromTopics(rateLimitsTopics);
        int maxOutboundUpdatesPerSecond = getMaxOutboundUpdatesPerSecondFromTopics(rateLimitsTopics);
        InboundRateLimiter inboundRateLimiter = topics.getContext().get(InboundRateLimiter.class);
        IotDataPlaneClientWrapper iotDataPlaneClientWrapper = topics.getContext().get(IotDataPlaneClientWrapper.class);

        return new RateLimitsConfiguration(maxLocalRequestRatePerThing, maxTotalLocalRequestRate,
                maxOutboundUpdatesPerSecond, inboundRateLimiter, iotDataPlaneClientWrapper);
    }

    /**
     * Creates a new rate limits configuration object and triggers updates based on previous configuration.
     *
     * @param oldConfiguration previous configuration of the component
     * @param serviceTopics    current configuration topics
     * @return rate limits configuration objects
     */
    public static RateLimitsConfiguration from(ComponentConfiguration oldConfiguration, Topics serviceTopics) {
        RateLimitsConfiguration rateLimitsConfiguration = getRateLimitsConfigurationFromTopics(serviceTopics);
        rateLimitsConfiguration.triggerUpdates(oldConfiguration);
        return rateLimitsConfiguration;
    }

    private void triggerUpdates(ComponentConfiguration oldComponentConfiguration) {
        RateLimitsConfiguration oldRateLimitsConfiguration = null;
        if (oldComponentConfiguration != null) {
            oldRateLimitsConfiguration = oldComponentConfiguration.rateLimitsConfiguration;
        }
        if (hasMaxLocalRequestPerThingChanged(oldRateLimitsConfiguration)) {
            inboundRateLimiter.setRate(maxLocalRequestRatePerThing);
        }
        if (hasMaxTotalLocalRequestRateChanged(oldRateLimitsConfiguration)) {
            inboundRateLimiter.setTotalRate(maxTotalLocalRequestRate);
        }
        if (hasMaxOutboundUpdatesPerSecondChanged(oldRateLimitsConfiguration)) {
            iotDataPlaneClientWrapper.setRate(maxOutboundUpdatesPerSecond);
        }
    }

    private boolean hasMaxLocalRequestPerThingChanged(RateLimitsConfiguration oldRateLimitsConfiguration) {
        return oldRateLimitsConfiguration == null
                || maxLocalRequestRatePerThing != oldRateLimitsConfiguration.getMaxLocalRequestRatePerThing();
    }

    private boolean hasMaxTotalLocalRequestRateChanged(RateLimitsConfiguration oldRateLimitsConfiguration) {
        return oldRateLimitsConfiguration == null
                || maxTotalLocalRequestRate != oldRateLimitsConfiguration.getMaxTotalLocalRequestRate();
    }

    private boolean hasMaxOutboundUpdatesPerSecondChanged(RateLimitsConfiguration oldRateLimitsConfiguration) {
        return oldRateLimitsConfiguration == null
                || maxOutboundUpdatesPerSecond != oldRateLimitsConfiguration.getMaxOutboundUpdatesPerSecond();
    }

}