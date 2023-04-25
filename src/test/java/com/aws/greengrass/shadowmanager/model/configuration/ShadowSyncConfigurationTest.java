/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.model.configuration;

import com.aws.greengrass.config.Configuration;
import com.aws.greengrass.config.Topics;
import com.aws.greengrass.dependency.Context;
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
import java.util.Map;
import java.util.stream.Stream;

import static com.aws.greengrass.componentmanager.KernelConfigResolver.CONFIGURATION_CONFIG_KEY;
import static com.aws.greengrass.shadowmanager.model.Constants.CLASSIC_SHADOW_IDENTIFIER;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_SYNCHRONIZATION_TOPIC;
import static com.aws.greengrass.shadowmanager.model.configuration.ShadowSyncConfigurationUtils.assertEqual;
import static com.aws.greengrass.shadowmanager.model.configuration.ShadowSyncConfigurationUtils.syncConfig;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@ExtendWith({MockitoExtension.class, GGExtension.class})
class ShadowSyncConfigurationTest {
    Configuration configuration;
    private final ObjectMapper mapper = new YAMLMapper();
    private static final String coreThingName = "coreThing";

    @BeforeEach
    void setup() {
        configuration = new Configuration(new Context());
    }

    @AfterEach
    void tearDown() {
        configuration.context.shutdown();
    }

    static Stream<Arguments> processConfigurationTests() {
        String classic = CLASSIC_SHADOW_IDENTIFIER;
        return Stream.of(
            arguments("config_empty.yaml", syncConfig(false)),
            arguments("config_add_on_interaction_enabled.yaml", syncConfig(true)),
            arguments("config.yaml", syncConfig(false,
                coreThingName, classic, false,
                coreThingName, "coreThingS1", false,
                coreThingName, "coreThingS2", false,
                coreThingName, "coreThingS3", true,
                "t1", classic, false,
                "t1", "s1", false,
                "t1", "s2", false,
                "t1", "s3", true,
                "t2", classic, true,
                "t2", "s1", true,
                "t3", classic, false,
                "t3", "s1", false,
                "t3", "s2", false,
                "t3", "s3", false
            ))
        );
    }

    @ParameterizedTest
    @MethodSource("processConfigurationTests")
    void GIVEN_config_WHEN_processConfiguration_THEN_processed(
        String configName,
        ShadowSyncConfiguration expectedConfig
    ) throws IOException, URISyntaxException {
        initConfig(configName);
        Topics syncTopics = configuration.getRoot().lookupTopics(
            CONFIGURATION_CONFIG_KEY, CONFIGURATION_SYNCHRONIZATION_TOPIC
        );
        ShadowSyncConfiguration actualConfig = ShadowSyncConfiguration.processConfiguration(
            syncTopics.toPOJO(), coreThingName
        ) ;

        assertEqual(actualConfig, expectedConfig);
    }

    public void initConfig(String configName) throws IOException, URISyntaxException {
        File f = new File(getClass().getResource(configName).toURI());
        byte[] configByteData = Files.readAllBytes(f.toPath());
        String configStr = new String(configByteData, StandardCharsets.UTF_8);

        configuration.mergeMap(0, mapper.readValue(configStr, Map.class));
        configuration.context.waitForPublishQueueToClear();
    }
}