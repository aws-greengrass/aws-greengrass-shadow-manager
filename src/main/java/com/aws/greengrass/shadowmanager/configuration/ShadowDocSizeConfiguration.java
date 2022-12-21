/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.configuration;

import com.aws.greengrass.config.Topics;
import com.aws.greengrass.shadowmanager.exception.InvalidConfigurationException;
import com.aws.greengrass.util.Coerce;
import lombok.Getter;

import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_MAX_DOC_SIZE_LIMIT_B_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.DEFAULT_DOCUMENT_SIZE;
import static com.aws.greengrass.shadowmanager.model.Constants.MAX_SHADOW_DOCUMENT_SIZE;

public final class ShadowDocSizeConfiguration {
    @Getter
    private final int maxShadowDocSizeConfiguration;

    private ShadowDocSizeConfiguration(int maxShadowDocSizeConfiguration) {
        this.maxShadowDocSizeConfiguration = maxShadowDocSizeConfiguration;
    }

    /**
     * Creates a new shadow doc size configuration object and triggers updates based on previous configuration.
     *
     * @param serviceTopics    current configuration topics
     * @return rate limits configuration objects
     */
    public static ShadowDocSizeConfiguration from(Topics serviceTopics) {
        return new ShadowDocSizeConfiguration(getMaxShadowDocSizeFromConfig(serviceTopics));
    }

    private static int getMaxShadowDocSizeFromConfig(Topics topics) {
        int newMaxShadowSize = Coerce.toInt(
                topics.findOrDefault(DEFAULT_DOCUMENT_SIZE, CONFIGURATION_MAX_DOC_SIZE_LIMIT_B_TOPIC));
        if (MAX_SHADOW_DOCUMENT_SIZE < newMaxShadowSize || newMaxShadowSize <= 0) {
            throw new InvalidConfigurationException(String.format(
                    "Maximum shadow size provided %d is either less than 0 "
                            + "or exceeds default maximum shadow size of %d",
                    newMaxShadowSize,
                    MAX_SHADOW_DOCUMENT_SIZE));
        }
        return newMaxShadowSize;
    }
}
