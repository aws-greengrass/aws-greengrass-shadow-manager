/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync;

import com.aws.greengrass.config.Configuration;
import com.aws.greengrass.config.Topics;
import com.aws.greengrass.config.WhatHappened;
import com.aws.greengrass.dependency.Context;
import com.aws.greengrass.shadowmanager.model.configuration.ShadowSyncConfiguration;
import com.aws.greengrass.shadowmanager.model.configuration.ThingShadowSyncConfiguration;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static com.aws.greengrass.componentmanager.KernelConfigResolver.CONFIGURATION_CONFIG_KEY;
import static com.aws.greengrass.shadowmanager.model.Constants.CLASSIC_SHADOW_IDENTIFIER;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_SYNCHRONIZATION_TOPIC;
import static com.aws.greengrass.shadowmanager.model.configuration.ShadowSyncConfigurationUtils.assertEqual;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@ExtendWith({MockitoExtension.class, GGExtension.class})
public class SyncConfigurationUpdaterTest {
    private static final String coreThingName = "coreThing";
    Configuration configuration;
    SyncConfigurationUpdater syncConfigurationUpdater;
    ObjectMapper mapper = new YAMLMapper();

    AtomicBoolean configInitialized;
    AtomicBoolean syncConfigSubscriberTriggered;
    Set<ThingShadowSyncConfiguration> syncConfigurations;

    @BeforeEach
    void setup() {
        configuration = new Configuration(new Context());
        syncConfigurations = new HashSet<>();
        syncConfigurationUpdater = new SyncConfigurationUpdater();
        syncConfigurationUpdater.setTopics(configuration.getRoot());

        syncConfigSubscriberTriggered = new AtomicBoolean(false);
        configInitialized = new AtomicBoolean(false);
        configuration.lookupTopics(CONFIGURATION_CONFIG_KEY).subscribe((what, newv) -> {
            if (!configInitialized.get()) {
                return;
            }
            if (what.equals(WhatHappened.timestampUpdated) || what.equals(WhatHappened.interiorAdded)) {
                return;
            }
            syncConfigSubscriberTriggered.set(true);
        });
    }

    @AfterEach
    void tearDown() {
        configuration.context.shutdown();
    }

    final void assertSyncConfigurations(Set<ThingShadowSyncConfiguration> expected) {
        Set<ThingShadowSyncConfiguration> actual =
            ShadowSyncConfiguration.processConfiguration(configuration.lookupTopics(CONFIGURATION_CONFIG_KEY,
                CONFIGURATION_SYNCHRONIZATION_TOPIC).toPOJO(), coreThingName).getSyncConfigurations();
        assertEqual(actual, expected);
    }

    void initConfig(String configName) throws IOException, URISyntaxException {
        String p = String.format("add_on_interaction/%s", configName);
        File f = new File(getClass().getResource(p).toURI());
        byte[] configByteData = Files.readAllBytes(f.toPath());
        String configStr = new String(configByteData, StandardCharsets.UTF_8);

        configuration.mergeMap(0, mapper.readValue(configStr, Map.class));
        Topics syncTopics = configuration.getRoot().lookupTopics(CONFIGURATION_CONFIG_KEY,
            CONFIGURATION_SYNCHRONIZATION_TOPIC);
        syncConfigurations = ShadowSyncConfiguration
            .processConfiguration(syncTopics.toPOJO(), coreThingName)
            .getSyncConfigurations();
        configuration.context.waitForPublishQueueToClear();
        configInitialized.set(true);
    }

    static Stream<Arguments> updateThingShadowsAddedOnInteractionAddTests() {
        return Stream.of(
            // config is empty
            arguments("config_empty.yaml", "thing1", CLASSIC_SHADOW_IDENTIFIER),
            arguments("config_empty.yaml", "thing1", "newShadow"),
            arguments("config_both.yaml", "existingThing", "newShadow"),
            arguments("config_both.yaml", "newThing", "newShadow"),
            arguments("config_non_interaction.yaml", "thing1", "newShadow"),
            arguments("config_on_interaction.yaml", "thing1", "newShadow")
        );
    }

    @ParameterizedTest
    @MethodSource("updateThingShadowsAddedOnInteractionAddTests")
    void GIVEN_config_WHEN_updateThingShadowsAddedOnInteraction_adds_THEN_added(
        String configName,
        String thingName,
        String shadowName
    ) throws IOException, URISyntaxException {
        initConfig(configName);
        Set<ThingShadowSyncConfiguration> desiredState = new HashSet<>(syncConfigurations);
        desiredState.add(ThingShadowSyncConfiguration
            .builder()
            .addedOnInteraction(true)
            .thingName(thingName)
            .shadowName(shadowName)
            .build()
        );

        syncConfigurationUpdater.updateThingShadowsAddedOnInteraction(desiredState);

        configuration.context.waitForPublishQueueToClear();
        assertThat("subscriber triggered", syncConfigSubscriberTriggered.get(), is(true));
        assertSyncConfigurations(desiredState);
    }

    static Stream<Arguments> updateThingShadowsAddedOnInteractionRemoveTests() {
        return Stream.of(
            arguments("config_both.yaml", "existingThing", "existingShadow2"),
            arguments("config_on_interaction.yaml", "existingThing", "existingShadow")
        );
    }

    @ParameterizedTest
    @MethodSource("updateThingShadowsAddedOnInteractionRemoveTests")
    void GIVEN_config_WHEN_updateThingShadowsAddedOnInteraction_removes_THEN_removed(
        String configName,
        String thingName,
        String shadowName
    ) throws IOException, URISyntaxException {
        initConfig(configName);
        Set<ThingShadowSyncConfiguration> desiredState = new HashSet<>(syncConfigurations);
        desiredState.remove(ThingShadowSyncConfiguration
            .builder()
            .addedOnInteraction(true)
            .thingName(thingName)
            .shadowName(shadowName)
            .build()
        );

        syncConfigurationUpdater.updateThingShadowsAddedOnInteraction(desiredState);

        configuration.context.waitForPublishQueueToClear();
        assertThat("subscriber triggered", syncConfigSubscriberTriggered.get(), is(true));
        assertSyncConfigurations(desiredState);
    }
}
