/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.model.configuration;

import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.shadowmanager.exception.InvalidConfigurationException;
import com.aws.greengrass.shadowmanager.exception.InvalidRequestParametersException;
import com.aws.greengrass.shadowmanager.util.Validator;
import com.aws.greengrass.util.Coerce;
import com.aws.greengrass.util.Pair;
import com.aws.greengrass.util.Utils;
import lombok.Builder;
import lombok.Getter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static com.aws.greengrass.shadowmanager.model.Constants.CLASSIC_SHADOW_IDENTIFIER;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_CLASSIC_SHADOW_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_CORE_THING_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_NAMED_SHADOWS_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_SHADOW_DOCUMENTS_MAP_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_SHADOW_DOCUMENTS_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_THING_NAME_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.UNEXPECTED_TYPE_FORMAT;
import static com.aws.greengrass.shadowmanager.model.Constants.UNEXPECTED_VALUE_FORMAT;

@Builder
@Getter
public class ShadowSyncConfiguration {
    private static final Logger logger = LogManager.getLogger(ShadowSyncConfiguration.class);

    private final Map<ThingShadow, ThingShadowSyncConfiguration> syncConfigurations;

    @Override
    public boolean equals(Object o) {
        // If the object is compared with itself then return true
        if (o == this) {
            return true;
        }
        // Check if o is an instance of ShadowSyncConfiguration or not "null instanceof [type]" also returns false
        if (!(o instanceof ShadowSyncConfiguration)) {
            return false;
        }

        // typecast o to ShadowSyncConfiguration so that we can compare data members
        ShadowSyncConfiguration newConfiguration = (ShadowSyncConfiguration) o;

        // Compare the data members and return accordingly
        return Objects.equals(this.syncConfigurations, newConfiguration.syncConfigurations);
    }

    @SuppressWarnings("PMD.UselessOverridingMethod")
    @Override
    public int hashCode() {
        return super.hashCode();
    }

    /**
     * Processes the Shadow sync configuration from POJO.
     *
     * @param configTopicsPojo the configuration POJO.
     * @param thingName        the Nucleus (device) thing name.
     * @return the Shadow sync configuration object.
     * @throws InvalidConfigurationException if the configuration is bad.
     */
    public static ShadowSyncConfiguration processConfiguration(Map<String, Object> configTopicsPojo, String thingName) {
        Map<ThingShadow, ThingShadowSyncConfiguration> syncConfigurationMap = new HashMap<>();
        try {
            processCoreThingConfiguration(configTopicsPojo, thingName, syncConfigurationMap);
            processOtherThingConfigurations(configTopicsPojo, syncConfigurationMap);
        } catch (InvalidRequestParametersException e) {
            throw new InvalidConfigurationException(e);
        }

        return ShadowSyncConfiguration.builder()
                .syncConfigurations(syncConfigurationMap)
                .build();
    }

    /**
     * Processes the device thing configuration.
     *
     * @param configTopicsPojo     The POJO for the configuration topic of device thing.
     * @param syncConfigurationMap the sync configuration map to add the device thing configuration to.
     * @throws InvalidRequestParametersException if the named shadow validation fails.
     * @implNote The implementation can handle the synchronize object being both a map and a list. The map approach is
     *     preferred since it is easier to add/remove sync configuration.
     */
    private static void processOtherThingConfigurations(
        Map<String, Object> configTopicsPojo,
        Map<ThingShadow, ThingShadowSyncConfiguration> syncConfigurationMap
    ) {
        // Process the shadow documents map first.
        final boolean isMapPresent = configTopicsPojo.containsKey(CONFIGURATION_SHADOW_DOCUMENTS_MAP_TOPIC);
        configTopicsPojo.computeIfPresent(CONFIGURATION_SHADOW_DOCUMENTS_MAP_TOPIC,
                (ignored, shadowDocumentsObject) -> {
                    if (shadowDocumentsObject instanceof Map) {
                        Map<String, Object> shadowDocumentsMap = (Map) shadowDocumentsObject;
                        shadowDocumentsMap.forEach((componentName, componentConfigObject) ->
                                processThingConfiguration(componentConfigObject, componentName, syncConfigurationMap,
                                        CONFIGURATION_SHADOW_DOCUMENTS_MAP_TOPIC));
                    } else {
                        throw new InvalidConfigurationException(String.format("Unexpected type in %s: %s",
                                CONFIGURATION_SHADOW_DOCUMENTS_MAP_TOPIC,
                                shadowDocumentsObject.getClass().getTypeName()));
                    }
                    return shadowDocumentsObject;
                });

        // Then process the shadow documents list.
        configTopicsPojo.computeIfPresent(CONFIGURATION_SHADOW_DOCUMENTS_TOPIC, (ignored, shadowDocumentsObject) -> {
            if (shadowDocumentsObject instanceof List) {
                if (isMapPresent) {
                    logger.atWarn().log("Both map and list synchronize configurations exist. "
                            + "Consider using the map since it is easier to add/remove sync configuration");
                }
                List<Object> shadowDocumentsToSyncList = (List) shadowDocumentsObject;
                shadowDocumentsToSyncList.forEach(shadowDocumentsToSync ->
                        processThingConfiguration(shadowDocumentsToSync, syncConfigurationMap));
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
     * @param configTopicsPojo     The POJO for the configuration topic of nucleus thing.
     * @param thingName            The nucleus thing name.
     * @param syncConfigurationMap the sync configuration map to add the nucleus thing configuration to.
     * @throws InvalidRequestParametersException if the named shadow validation fails.
     */
    private static void processCoreThingConfiguration(
        Map<String, Object> configTopicsPojo,
        String thingName,
        Map<ThingShadow, ThingShadowSyncConfiguration> syncConfigurationMap) {
        configTopicsPojo.computeIfPresent(CONFIGURATION_CORE_THING_TOPIC, (ignored, coreThingConfigObject) -> {
            processThingConfiguration(
                coreThingConfigObject,
                thingName,
                syncConfigurationMap,
                CONFIGURATION_CORE_THING_TOPIC
            );
            return coreThingConfigObject;
        });
    }

    /**
     * Processes the thing configuration if presented in a map format.
     *
     * @param thingConfigObject    The thing configuration object
     * @param thingName            The thing name
     * @param syncConfigurationMap the sync configuration map to add the nucleus thing configuration to.
     * @throws InvalidConfigurationException if the named shadow validation fails.
     */
    private static void processThingConfiguration(
        Object thingConfigObject,
        String thingName,
        Map<ThingShadow, ThingShadowSyncConfiguration> syncConfigurationMap,
        String configName
    ) {
        if (thingConfigObject instanceof Map) {
            Map<String, Object> thingConfig = (Map) thingConfigObject;
            processThingShadowSyncConfiguration(syncConfigurationMap, thingConfig, thingName);
        } else {
            throw new InvalidConfigurationException(String.format(UNEXPECTED_TYPE_FORMAT,
                    configName, thingConfigObject.getClass().getTypeName()));
        }
    }

    /**
     * Processes the thing configuration if presented in a list format.
     *
     * @param thingConfigObject    The thing configuration object
     * @param syncConfigurationMap the sync configuration map to add the nucleus thing configuration to.
     * @throws InvalidRequestParametersException if the named shadow validation fails.
     */
    private static void processThingConfiguration(
        Object thingConfigObject,
        Map<ThingShadow, ThingShadowSyncConfiguration> syncConfigurationMap
    ) {
        if (thingConfigObject instanceof Map) {
            Map<String, Object> thingConfig = (Map) thingConfigObject;
            processThingShadowSyncConfiguration(syncConfigurationMap, thingConfig, getThingName(thingConfig));
        } else {
            throw new InvalidConfigurationException(String.format(UNEXPECTED_TYPE_FORMAT,
                    CONFIGURATION_SHADOW_DOCUMENTS_TOPIC, thingConfigObject.getClass().getTypeName()));
        }
    }

    /**
     * Gets the thing name from the thing configuration map.
     *
     * @param thingConfig The thing configuration map
     * @throws InvalidConfigurationException if thing name is not present or invalid.
     */
    private static String getThingName(Map<String, Object> thingConfig) {
        Object name = null;
        if (thingConfig.containsKey(CONFIGURATION_THING_NAME_TOPIC)) {
            name = thingConfig.get(CONFIGURATION_THING_NAME_TOPIC);
            if (name instanceof String) {
                String thingName = Coerce.toString(name);
                if (Utils.isEmpty(thingName)) {
                    throw new InvalidConfigurationException(String.format(UNEXPECTED_VALUE_FORMAT,
                            CONFIGURATION_THING_NAME_TOPIC, thingName));
                }
                return thingName;
            }
        }
        throw new InvalidConfigurationException(String.format(UNEXPECTED_TYPE_FORMAT,
                CONFIGURATION_THING_NAME_TOPIC, name == null ? null : name.getClass().getTypeName()));

    }

    /**
     * Process the thing configuration map and add the new ThingShadowSyncConfiguration reference in the set.
     *
     * @param syncConfigurationMap the sync configuration map to add the nucleus thing configuration to.
     * @param thingConfig          The thing configuration map.
     * @param thingName            The thing name.
     * @throws InvalidConfigurationException if named shadows list is in a bad format.
     */
    private static void processThingShadowSyncConfiguration(
        Map<ThingShadow, ThingShadowSyncConfiguration> syncConfigurationMap,
        Map<String, Object> thingConfig,
        String thingName
    ) {
        Validator.validateThingName(thingName);

        ThingShadowSyncConfiguration syncConfiguration;
        boolean syncClassicTopic = true;
        for (Map.Entry<String, Object> configObjectEntry : thingConfig.entrySet()) {
            switch (configObjectEntry.getKey()) {
                case CONFIGURATION_CLASSIC_SHADOW_TOPIC:
                    syncClassicTopic = Coerce.toBoolean(configObjectEntry.getValue());
                    break;
                case CONFIGURATION_NAMED_SHADOWS_TOPIC:
                    if (configObjectEntry.getValue() instanceof List) {
                        List<String> namedShadows = Coerce.toStringList(configObjectEntry.getValue());
                        for (String namedShadow : namedShadows) {
                            Validator.validateShadowName(namedShadow);
                            syncConfiguration = ThingShadowSyncConfiguration.builder()
                                    .thingName(thingName)
                                    .shadowName(namedShadow)
                                    .build();
                            syncConfigurationMap.put(
                                syncConfiguration.toThingShadow(),
                                syncConfiguration
                            );
                        }
                    } else {
                        throw new InvalidConfigurationException(String.format(UNEXPECTED_TYPE_FORMAT,
                                configObjectEntry.getKey(), configObjectEntry.getClass().getTypeName()));
                    }
                    break;
                default:
                    // Do nothing here since we want to be lenient with unknown fields.
                    break;
            }
        }
        if (syncClassicTopic) {
            syncConfiguration = ThingShadowSyncConfiguration.builder()
                    .thingName(thingName)
                    .shadowName(CLASSIC_SHADOW_IDENTIFIER)
                    .build();
            syncConfigurationMap.put(syncConfiguration.toThingShadow(), syncConfiguration);
        }
    }

    /**
     * Returns the set of shadows to be synced.
     *
     * @return Set of shadows to be synced.
     */
    public Set<Pair<String, String>> getSyncShadows() {
        return syncConfigurations.keySet().stream().map(thingShadow ->
                new Pair<>(thingShadow.getThingName(), thingShadow.getShadowName()))
                .collect(Collectors.toSet());
    }
}
