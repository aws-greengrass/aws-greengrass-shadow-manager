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

    private RateLimitsConfiguration(Topics topics) {
        Topics rateLimitsTopics = topics.lookupTopics(CONFIGURATION_RATE_LIMITS_TOPIC);
        maxTotalLocalRequestRate = getMaxTotalLocalRequestRateFromTopics(rateLimitsTopics);
        maxLocalRequestRatePerThing = getMaxLocalRequestRatePerThingFromTopics(rateLimitsTopics);
        maxOutboundUpdatesPerSecond = getMaxOutboundUpdatesPerSecondFromTopics(rateLimitsTopics);
        inboundRateLimiter = topics.getContext().get(InboundRateLimiter.class);
        iotDataPlaneClientWrapper = topics.getContext().get(IotDataPlaneClientWrapper.class);
    }

    public static RateLimitsConfiguration from(Topics serviceTopics) {
        return new RateLimitsConfiguration(serviceTopics);
    }

    /**
     * Updates request rate limits with the latest configuration of the component.
     */
    public void triggerUpdates() {
        inboundRateLimiter.setRate(maxLocalRequestRatePerThing);
        inboundRateLimiter.setTotalRate(maxTotalLocalRequestRate);
        iotDataPlaneClientWrapper.setRate(maxOutboundUpdatesPerSecond);
    }

    /**
     * Compares current configuration with the previous configuration.
     *
     * @param oldRateLimitsConfiguration previous rate limits configuration
     * @return True if the configurations are different
     */
    public boolean hasChanged(RateLimitsConfiguration oldRateLimitsConfiguration) {
        return !this.equals(oldRateLimitsConfiguration);
    }
}
