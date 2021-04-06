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
        boolean provideSyncStatus = DEFAULT_PROVIDE_SYNC_STATUS;
        int maxOutboundSyncUpdatesPerSecond = DEFAULT_MAX_OUTBOUND_SYNC_UPDATES_PS;

        processNucleusThingConfiguration(configTopicsPojo, thingName, syncConfigurationList);
        processOtherThingConfigurations(configTopicsPojo, syncConfigurationList);

        if (configTopicsPojo.containsKey(CONFIGURATION_MAX_OUTBOUND_UPDATES_PS_TOPIC)) {
            //TODO: Do we do a validation here to check for the AWS region and base the TPS on that?
            maxOutboundSyncUpdatesPerSecond = Coerce.toInt(configTopicsPojo
                    .get(CONFIGURATION_MAX_OUTBOUND_UPDATES_PS_TOPIC));
        }

        if (configTopicsPojo.containsKey(CONFIGURATION_PROVIDE_SYNC_STATUS_TOPIC)) {
            provideSyncStatus = Coerce.toBoolean(configTopicsPojo.get(CONFIGURATION_PROVIDE_SYNC_STATUS_TOPIC));
        }

        return ShadowSyncConfiguration.builder()
                .syncConfigurationList(syncConfigurationList)
                .maxOutboundSyncUpdatesPerSecond(maxOutboundSyncUpdatesPerSecond)
                .provideSyncStatus(provideSyncStatus)
                .build();
    }

    private static void processOtherThingConfigurations(Map<String, Object> configTopicsPojo,
                                                        List<ThingShadowSyncConfiguration> syncConfigurationList) {
        configTopicsPojo.computeIfPresent(CONFIGURATION_SHADOW_DOCUMENTS_TOPIC, (s, o) -> {
            if (o instanceof ArrayList) {
                List<Object> shadowDocumentsToSyncList = (ArrayList) o;
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
                        CONFIGURATION_SHADOW_DOCUMENTS_TOPIC, o.getClass().getTypeName()));
            }
            return o;
        });
    }

    private static void processNucleusThingConfiguration(Map<String, Object> configTopicsPojo, String thingName,
                                                         List<ThingShadowSyncConfiguration> syncConfigurationList) {
        configTopicsPojo.computeIfPresent(CONFIGURATION_NUCLEUS_THING_TOPIC, (s, o) -> {
            if (o instanceof Map) {
                Map<String, Object> nucleusThingConfig = (Map) o;
                ThingShadowSyncConfiguration.ThingShadowSyncConfigurationBuilder builder = ThingShadowSyncConfiguration
                        .builder()
                        .thingName(thingName);
                nucleusThingConfig.forEach((s1, o1) -> {
                    switch (s1) {
                        case CONFIGURATION_CLASSIC_SHADOW_TOPIC:
                            builder.syncClassicShadow(Coerce.toBoolean(o1));
                            break;
                        case CONFIGURATION_NAMED_SHADOWS_TOPIC:
                            builder.syncNamedShadows(Coerce.toStringList(o1));
                            break;
                        default:
                            throw new InvalidConfigurationException(String.format("Unexpected value in %s: %s",
                                    CONFIGURATION_NUCLEUS_THING_TOPIC, s1));
                    }
                });
                syncConfigurationList.add(builder.build());
            } else {
                throw new InvalidConfigurationException(String.format("Unexpected type in %s: %s",
                        CONFIGURATION_SHADOW_DOCUMENTS_TOPIC, o.getClass().getTypeName()));
            }
            return o;
        });
    }
}
