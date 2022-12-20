/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.configuration;

import com.aws.greengrass.config.Topics;

import static com.aws.greengrass.componentmanager.KernelConfigResolver.CONFIGURATION_CONFIG_KEY;

public class ComponentConfiguration {
    RateLimitsConfiguration rateLimitsConfiguration;

    public ComponentConfiguration(RateLimitsConfiguration rateLimitsConfiguration) {
        this.rateLimitsConfiguration = rateLimitsConfiguration;
    }

    /**
     * Constructor for shadow manager component.
     * @param oldConfiguration previous configuration of the component
     * @param updatedTopics current configuration topics
     * @return shadow manager component configuration object
     */
    public static ComponentConfiguration from(ComponentConfiguration oldConfiguration, Topics updatedTopics) {
        Topics serviceConfiguration = updatedTopics.lookupTopics(CONFIGURATION_CONFIG_KEY);
        RateLimitsConfiguration rateLimitsConfiguration = RateLimitsConfiguration.from(serviceConfiguration);
        ComponentConfiguration newConfiguration = new ComponentConfiguration(rateLimitsConfiguration);
        newConfiguration.triggerUpdates(oldConfiguration);
        return newConfiguration;
    }

    private void triggerUpdates(ComponentConfiguration oldConfiguration) {
        if (this.equals(oldConfiguration)) {
            return;
        }
        if (hasRateLimitsChanged(oldConfiguration)) {
            rateLimitsConfiguration.triggerUpdates();
        }
    }

    private boolean hasRateLimitsChanged(ComponentConfiguration oldConfiguration) {
        if (oldConfiguration == null) {
            return true;
        }
        return rateLimitsConfiguration.hasChanged(oldConfiguration.rateLimitsConfiguration);
    }
}
