/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.configuration;

import com.aws.greengrass.config.Topics;
import lombok.Getter;

import static com.aws.greengrass.componentmanager.KernelConfigResolver.CONFIGURATION_CONFIG_KEY;

public final class ComponentConfiguration {
    @Getter
    private final RateLimitsConfiguration rateLimitsConfiguration;
    @Getter
    private final ShadowDocSizeConfiguration shadowDocSizeConfiguration;

    private ComponentConfiguration(RateLimitsConfiguration rateLimitsConfiguration,
                                   ShadowDocSizeConfiguration shadowDocSizeConfiguration) {
        this.rateLimitsConfiguration = rateLimitsConfiguration;
        this.shadowDocSizeConfiguration = shadowDocSizeConfiguration;
    }

    /**
     * Constructor for shadow manager component.
     * @param oldConfiguration previous configuration of the component
     * @param updatedTopics current configuration topics
     * @return shadow manager component configuration object
     */
    public static ComponentConfiguration from(ComponentConfiguration oldConfiguration, Topics updatedTopics) {
        Topics serviceTopics = updatedTopics.lookupTopics(CONFIGURATION_CONFIG_KEY);
        RateLimitsConfiguration rateLimitsConfiguration = RateLimitsConfiguration.from(serviceTopics);
        ShadowDocSizeConfiguration shadowDocSizeConfiguration = ShadowDocSizeConfiguration.from(serviceTopics);
        return new ComponentConfiguration(rateLimitsConfiguration, shadowDocSizeConfiguration);
    }
}
