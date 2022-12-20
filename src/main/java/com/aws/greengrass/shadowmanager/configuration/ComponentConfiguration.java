/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.configuration;

import com.aws.greengrass.config.Topics;

import static com.aws.greengrass.componentmanager.KernelConfigResolver.CONFIGURATION_CONFIG_KEY;

public final class ComponentConfiguration {
    RateLimitsConfiguration rateLimitsConfiguration;

    private ComponentConfiguration(RateLimitsConfiguration rateLimitsConfiguration) {
        this.rateLimitsConfiguration = rateLimitsConfiguration;
    }

    /**
     * Constructor for shadow manager component.
     * @param oldConfiguration previous configuration of the component
     * @param updatedTopics current configuration topics
     * @return shadow manager component configuration object
     */
    public static ComponentConfiguration from(ComponentConfiguration oldConfiguration, Topics updatedTopics) {
        Topics serviceTopics = updatedTopics.lookupTopics(CONFIGURATION_CONFIG_KEY);
        RateLimitsConfiguration rateLimitsConfiguration = RateLimitsConfiguration.from(
                getOldRateLimitsConfiguration(oldConfiguration), serviceTopics);
        return new ComponentConfiguration(rateLimitsConfiguration);
    }

    private static RateLimitsConfiguration getOldRateLimitsConfiguration(ComponentConfiguration oldComponentConfig) {
        if (oldComponentConfig != null) {
            return oldComponentConfig.rateLimitsConfiguration;
        }
        return null;
    }
}
