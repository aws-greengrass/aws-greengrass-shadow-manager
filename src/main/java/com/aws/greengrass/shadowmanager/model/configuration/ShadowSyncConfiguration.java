/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.model.configuration;

import com.aws.greengrass.shadowmanager.exception.InvalidConfigurationException;
import com.aws.greengrass.shadowmanager.util.JsonUtil;
import com.aws.greengrass.util.Coerce;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_CLASSIC_SHADOW_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_MAX_OUTBOUND_UPDATES_PS_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_NAMED_SHADOWS_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_NUCLEUS_THING_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_PROVIDE_SYNC_STATUS_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_SHADOW_DOCUMENTS_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.DEFAULT_MAX_OUTBOUND_SYNC_UPDATES_PS;
import static com.aws.greengrass.shadowmanager.model.Constants.DEFAULT_PROVIDE_SYNC_STATUS;

@Builder
@Getter
public class ShadowSyncConfiguration {
    private final List<ThingShadowSyncConfiguration> syncConfigurationList;
    @JsonProperty(CONFIGURATION_PROVIDE_SYNC_STATUS_TOPIC)
    private final boolean provideSyncStatus;
    @JsonProperty(CONFIGURATION_MAX_OUTBOUND_UPDATES_PS_TOPIC)
    private final int maxOutboundSyncUpdatesPerSecond;

    /**
     * Processes the Shadow sync configuration from POJO.
     *
     * @param configTopicsPojo the configuration POJO.
     * @param thingName        the Nucleus (device) thing name.
     * @return the Shadow sync configuration object.
     * @throws InvalidConfigurationException if the configuration is bad.
     */
    public static ShadowSyncConfiguration processConfiguration(Map<String, Object> configTopicsPojo, String thingName) {
        List<ThingShadowSyncConfiguration> syncConfigurationList = new ArrayList<>();
        processNucleusThingConfiguration(configTopicsPojo, thingName, syncConfigurationList);
        processOtherThingConfigurations(configTopicsPojo, syncConfigurationList);

        //TODO: Do we do a validation here to check for the AWS region and base the TPS on that?
        final int maxOutboundSyncUpdatesPerSecond = Optional.ofNullable(
                configTopicsPojo.get(CONFIGURATION_MAX_OUTBOUND_UPDATES_PS_TOPIC))
                .map(Coerce::toInt)
                .orElse(DEFAULT_MAX_OUTBOUND_SYNC_UPDATES_PS);
        final boolean provideSyncStatus = Optional.ofNullable(
                configTopicsPojo.get(CONFIGURATION_PROVIDE_SYNC_STATUS_TOPIC))
                .map(Coerce::toBoolean)
                .orElse(DEFAULT_PROVIDE_SYNC_STATUS);

        return ShadowSyncConfiguration.builder()
                .syncConfigurationList(syncConfigurationList)
                .maxOutboundSyncUpdatesPerSecond(maxOutboundSyncUpdatesPerSecond)
                .provideSyncStatus(provideSyncStatus)
                .build();
    }

    private static void processOtherThingConfigurations(Map<String, Object> configTopicsPojo,
                                                        List<ThingShadowSyncConfiguration> syncConfigurationList) {
        configTopicsPojo.computeIfPresent(CONFIGURATION_SHADOW_DOCUMENTS_TOPIC, (ignored, shadowDocumentsObject) -> {
            if (shadowDocumentsObject instanceof ArrayList) {
                List<Object> shadowDocumentsToSyncList = (List) shadowDocumentsObject;
                shadowDocumentsToSyncList.forEach(shadowDocumentsToSync -> {
                    if (shadowDocumentsToSync instanceof Map) {
                        try {
                            ThingShadowSyncConfiguration syncConfiguration = JsonUtil.OBJECT_MAPPER
                                    .convertValue(shadowDocumentsToSync, ThingShadowSyncConfiguration.class);
                            syncConfigurationList.add(syncConfiguration);
                        } catch (IllegalArgumentException e) {
                            throw new InvalidConfigurationException(e);
                        }
                    }
                });
            } else {
                throw new InvalidConfigurationException(String.format("Unexpected type in %s: %s",
                        CONFIGURATION_SHADOW_DOCUMENTS_TOPIC, shadowDocumentsObject.getClass().getTypeName()));
            }
            return shadowDocumentsObject;
        });
    }

    private static void processNucleusThingConfiguration(Map<String, Object> configTopicsPojo, String thingName,
                                                         List<ThingShadowSyncConfiguration> syncConfigurationList) {
        configTopicsPojo.computeIfPresent(CONFIGURATION_NUCLEUS_THING_TOPIC, (ignored, nucleusThingConfigObject) -> {
            if (nucleusThingConfigObject instanceof Map) {
                Map<String, Object> nucleusThingConfig = (Map) nucleusThingConfigObject;
                ThingShadowSyncConfiguration.ThingShadowSyncConfigurationBuilder builder = ThingShadowSyncConfiguration
                        .builder()
                        .thingName(thingName);
                nucleusThingConfig.forEach((configFieldName, configFieldValue) -> {
                    switch (configFieldName) {
                        case CONFIGURATION_CLASSIC_SHADOW_TOPIC:
                            builder.syncClassicShadow(Coerce.toBoolean(configFieldValue));
                            break;
                        case CONFIGURATION_NAMED_SHADOWS_TOPIC:
                            builder.syncNamedShadows(Coerce.toStringList(configFieldValue));
                            break;
                        default:
                            throw new InvalidConfigurationException(String.format("Unexpected value in %s: %s",
                                    CONFIGURATION_NUCLEUS_THING_TOPIC, configFieldName));
                    }
                });
                syncConfigurationList.add(builder.build());
            } else {
                throw new InvalidConfigurationException(String.format("Unexpected type in %s: %s",
                        CONFIGURATION_SHADOW_DOCUMENTS_TOPIC, nucleusThingConfigObject.getClass().getTypeName()));
            }
            return nucleusThingConfigObject;
        });
    }
}
