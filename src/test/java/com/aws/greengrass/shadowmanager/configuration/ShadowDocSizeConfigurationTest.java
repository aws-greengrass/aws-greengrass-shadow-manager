/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.configuration;

import com.aws.greengrass.config.Topics;
import com.aws.greengrass.dependency.Context;
import com.aws.greengrass.shadowmanager.exception.InvalidConfigurationException;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.testcommons.testutilities.GGServiceTestUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;

import static com.aws.greengrass.componentmanager.KernelConfigResolver.CONFIGURATION_CONFIG_KEY;
import static com.aws.greengrass.shadowmanager.model.Constants.CONFIGURATION_MAX_DOC_SIZE_LIMIT_B_TOPIC;
import static com.aws.greengrass.shadowmanager.model.Constants.DEFAULT_DOCUMENT_SIZE;
import static com.aws.greengrass.shadowmanager.model.Constants.MAX_SHADOW_DOCUMENT_SIZE;
import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith({MockitoExtension.class, GGExtension.class})
class ShadowDocSizeConfigurationTest extends GGServiceTestUtil {
    private Topics configurationTopics;

    @BeforeEach
    void beforeEach() {
        configurationTopics = Topics.of(new Context(), CONFIGURATION_CONFIG_KEY, null);
    }

    @AfterEach
    void afterEach() throws IOException {
        configurationTopics.getContext().close();
    }

    @Test
    void GIVEN_default_configuration_WHEN_initialize_THEN_update_max_doc_size_to_default() {
        ShadowDocSizeConfiguration shadowDocSizeConfiguration = ShadowDocSizeConfiguration.from(configurationTopics);
        assertThat(shadowDocSizeConfiguration.getMaxShadowDocSizeConfiguration(), is(DEFAULT_DOCUMENT_SIZE));
    }

    @ParameterizedTest
    @ValueSource(ints = {DEFAULT_DOCUMENT_SIZE, MAX_SHADOW_DOCUMENT_SIZE})
    void GIVEN_good_max_doc_size_WHEN_initialize_THEN_updates_max_doc_size_correctly(int docSize) {
        configurationTopics.lookup(CONFIGURATION_MAX_DOC_SIZE_LIMIT_B_TOPIC).withValue(docSize);
        ShadowDocSizeConfiguration shadowDocSizeConfiguration = ShadowDocSizeConfiguration.from(configurationTopics);
        assertThat(shadowDocSizeConfiguration.getMaxShadowDocSizeConfiguration(), is(docSize));
    }


    @ParameterizedTest
    @ValueSource(ints = {MAX_SHADOW_DOCUMENT_SIZE + 1, -1})
    void GIVEN_bad_max_doc_size_WHEN_initialize_THEN_throws_exception(int docSize, ExtensionContext extensionContext) {
        ignoreExceptionOfType(extensionContext, InvalidConfigurationException.class);
        configurationTopics.lookup(CONFIGURATION_MAX_DOC_SIZE_LIMIT_B_TOPIC).withValue(docSize);
        assertThrows(InvalidConfigurationException.class, ()-> ShadowDocSizeConfiguration.from(configurationTopics));
    }

}
