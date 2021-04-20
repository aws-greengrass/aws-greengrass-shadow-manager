/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync;

import com.aws.greengrass.config.Topic;
import com.aws.greengrass.dependency.Context;
import com.aws.greengrass.deployment.DeviceConfiguration;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static com.aws.greengrass.deployment.DeviceConfiguration.DEVICE_PARAM_AWS_REGION;
import static com.aws.greengrass.deployment.DeviceConfiguration.DEVICE_PARAM_IOT_DATA_ENDPOINT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith({MockitoExtension.class, GGExtension.class})
class IotDataPlaneClientFactoryTest {
    private static final String mockKeyPath = "/test/keypath";
    private static final String mockCertPath = "/test/certpath";

    @Mock
    private DeviceConfiguration mockDeviceConfiguration;
    @Mock
    private Context mockContext;

    private IotDataPlaneClientFactory clientFactory;

    @BeforeEach
    void setup() {
        Topic dataEndpointTopic = Topic.of(mockContext, DEVICE_PARAM_IOT_DATA_ENDPOINT, "xxxxxx-ats.iot.us-west-2.amazonaws.com");
        when(mockDeviceConfiguration.getIotDataEndpoint()).thenReturn(dataEndpointTopic);
        Topic regionTopic = Topic.of(mockContext, DEVICE_PARAM_AWS_REGION, "us-west-2");
        when(mockDeviceConfiguration.getAWSRegion()).thenReturn(regionTopic);
        Topic privKeyTopicMock = mock(Topic.class);
        Topic pubKeyTopicMock = mock(Topic.class);

        when(privKeyTopicMock.getOnce()).thenReturn(mockKeyPath);
        when(pubKeyTopicMock.getOnce()).thenReturn(mockCertPath);

        when(mockDeviceConfiguration.getPrivateKeyFilePath()).thenReturn(privKeyTopicMock);
        when(mockDeviceConfiguration.getCertificateFilePath()).thenReturn(pubKeyTopicMock);
    }

    @Test
    void GIVEN_good_config_WHEN_configure_client_THEN_correctly_configures_iot_data_client() {
        clientFactory = new IotDataPlaneClientFactory(mockDeviceConfiguration);
        assertThat(clientFactory.getIotDataPlaneClient(), is(notNullValue()));

        verify(mockDeviceConfiguration, times(1)).getIotDataEndpoint();
        verify(mockDeviceConfiguration, times(1)).getAWSRegion();
        verify(mockDeviceConfiguration, times(1)).getPrivateKeyFilePath();
        verify(mockDeviceConfiguration, times(1)).getCertificateFilePath();
    }
}
