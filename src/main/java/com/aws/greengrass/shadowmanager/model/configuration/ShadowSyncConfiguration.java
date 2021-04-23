/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.model.configuration;

import com.aws.greengrass.shadowmanager.exception.InvalidConfigurationException;
import com.aws.greengrass.shadowmanager.exception.InvalidRequestParametersException;
import com.aws.greengrass.shadowmanager.util.Validator;
import com.aws.greengrass.util.Coerce;
import com.aws.greengrass.util.SerializerFactory;
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
    //TODO: Convert this into a map for optimized lookup. Key = thingName|shadowName
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
        int maxOutboundSyncUpdatesPerSecond = DEFAULT_MAX_OUTBOUND_SYNC_UPDATES_PS;
        try {
            processNucleusThingConfiguration(configTopicsPojo, thingName, syncConfigurationList);
            processOtherThingConfigurations(configTopicsPojo, syncConfigurationList);
        } catch (InvalidRequestParametersException e) {
            throw new InvalidConfigurationException(e);
        }

        if (configTopicsPojo.containsKey(CONFIGURATION_MAX_OUTBOUND_UPDATES_PS_TOPIC)) {
            int newMaxOutboundSyncUpdatesPerSecond = Coerce.toInt(configTopicsPojo
                    .get(CONFIGURATION_MAX_OUTBOUND_UPDATES_PS_TOPIC));
            Validator.validateOutboundSyncUpdatesPerSecond(newMaxOutboundSyncUpdatesPerSecond);
            maxOutboundSyncUpdatesPerSecond = newMaxOutboundSyncUpdatesPerSecond;
        }

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

    /**
     * Processes the device thing configuration.
     *
     * @param configTopicsPojo      The POJO for the configuration topic of device thing.
     * @param syncConfigurationList the sync configuration list to add the device thing configuration to.
     * @throws InvalidRequestParametersException if the named shadow validation fails.
     */
    private static void processOtherThingConfigurations(Map<String, Object> configTopicsPojo,
                                                        List<ThingShadowSyncConfiguration> syncConfigurationList) {
        configTopicsPojo.computeIfPresent(CONFIGURATION_SHADOW_DOCUMENTS_TOPIC, (ignored, shadowDocumentsObject) -> {
            if (shadowDocumentsObject instanceof ArrayList) {
                List<Object> shadowDocumentsToSyncList = (List) shadowDocumentsObject;
                shadowDocumentsToSyncList.forEach(shadowDocumentsToSync -> {
                    if (shadowDocumentsToSync instanceof Map) {
                        try {
                            ThingShadowSyncConfiguration syncConfiguration = SerializerFactory
                                    .getFailSafeJsonObjectMapper()
                                    .convertValue(shadowDocumentsToSync, ThingShadowSyncConfiguration.class);
                            Validator.validateThingName(syncConfiguration.getThingName());
                            for (String namedShadow : syncConfiguration.getSyncNamedShadows()) {
                                Validator.validateShadowName(namedShadow);
                            }
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

    /**
     * Processes the Nucleus thing configuration.
     *
     * @param configTopicsPojo      The POJO for the configuration topic of nucleus thing.
     * @param thingName             The nucleus thing name.
     * @param syncConfigurationList the sync configuration list to add the nucleus thing configuration to.
     * @throws InvalidRequestParametersException if the named shadow validation fails.
     */
    private static void processNucleusThingConfiguration(Map<String, Object> configTopicsPojo, String thingName,
                                                         List<ThingShadowSyncConfiguration> syncConfigurationList) {
        configTopicsPojo.computeIfPresent(CONFIGURATION_NUCLEUS_THING_TOPIC, (ignored, nucleusThingConfigObject) -> {
            if (nucleusThingConfigObject instanceof Map) {
                Map<String, Object> nucleusThingConfig = (Map) nucleusThingConfigObject;
                ThingShadowSyncConfiguration syncConfiguration = ThingShadowSyncConfiguration
                        .builder()
                        .isNucleusThing(true)
                        .thingName(thingName)
                        .build();
                for (Map.Entry<String, Object> configObjectEntry : nucleusThingConfig.entrySet()) {
                    switch (configObjectEntry.getKey()) {
                        case CONFIGURATION_CLASSIC_SHADOW_TOPIC:
                            syncConfiguration = syncConfiguration.toBuilder()
                                    .syncClassicShadow(Coerce.toBoolean(configObjectEntry.getValue())).build();
                            break;
                        case CONFIGURATION_NAMED_SHADOWS_TOPIC:
                            List<String> namedShadows = Coerce.toStringList(configObjectEntry.getValue());
                            for (String namedShadow : namedShadows) {
                                Validator.validateShadowName(namedShadow);
                            }
                            syncConfiguration = syncConfiguration.toBuilder()
                                    .syncNamedShadows(namedShadows)
                                    .build();
                            break;
                        default:
                            // Do nothing here since we want to be lenient with unknown fields.
                            break;
                    }
                }
                syncConfigurationList.add(syncConfiguration);
            } else {
                throw new InvalidConfigurationException(String.format("Unexpected type in %s: %s",
                        CONFIGURATION_SHADOW_DOCUMENTS_TOPIC, nucleusThingConfigObject.getClass().getTypeName()));
            }
            return nucleusThingConfigObject;
        });
    }
}
