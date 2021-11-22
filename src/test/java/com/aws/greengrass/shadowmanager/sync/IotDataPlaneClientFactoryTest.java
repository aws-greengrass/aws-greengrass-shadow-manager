/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync;

import com.aws.greengrass.config.ChildChanged;
import com.aws.greengrass.config.Topic;
import com.aws.greengrass.config.WhatHappened;
import com.aws.greengrass.dependency.Context;
import com.aws.greengrass.deployment.DeviceConfiguration;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;


import static com.aws.greengrass.deployment.DeviceConfiguration.DEVICE_PARAM_AWS_REGION;
import static com.aws.greengrass.deployment.DeviceConfiguration.DEVICE_PARAM_IOT_DATA_ENDPOINT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith({MockitoExtension.class, GGExtension.class})
class IotDataPlaneClientFactoryTest {
    private static final String mockKeyPath = "/test/keypath";
    private static final String mockCertPath = "/test/certpath";
    private static final String mockKeyPath2 = "/test/keypath2";
    private static final String mockCertPath2 = "/test/certpath2";

    @Mock
    private DeviceConfiguration mockDeviceConfiguration;
    @Mock
    private Context mockContext;
    @Captor
    private ArgumentCaptor<ChildChanged> ccCaptor;

    private IotDataPlaneClientFactory clientFactory;

    @BeforeEach
    void setup() {
        Topic dataEndpointTopic = Topic.of(mockContext, DEVICE_PARAM_IOT_DATA_ENDPOINT, "xxxxxx-ats.iot.us-west-2.amazonaws.com");
        when(mockDeviceConfiguration.getIotDataEndpoint()).thenReturn(dataEndpointTopic);
        Topic regionTopic = Topic.of(mockContext, DEVICE_PARAM_AWS_REGION, "us-west-2");
        when(mockDeviceConfiguration.getAWSRegion()).thenReturn(regionTopic);
        Topic privKeyTopicMock = mock(Topic.class);
        Topic pubKeyTopicMock = mock(Topic.class);

        when(privKeyTopicMock.getOnce()).thenReturn(mockKeyPath, mockKeyPath2);
        when(pubKeyTopicMock.getOnce()).thenReturn(mockCertPath, mockCertPath2);

        when(mockDeviceConfiguration.getPrivateKeyFilePath()).thenReturn(privKeyTopicMock);
        when(mockDeviceConfiguration.getCertificateFilePath()).thenReturn(pubKeyTopicMock);
        doNothing().when(mockDeviceConfiguration).onAnyChange(ccCaptor.capture());
    }

    @Test
    void GIVEN_good_config_WHEN_configure_client_THEN_correctly_configures_iot_data_client() {
        clientFactory = new IotDataPlaneClientFactory(mockDeviceConfiguration);
        assertThat(clientFactory.getIotDataPlaneClient(), is(notNullValue()));

        verify(mockDeviceConfiguration, times(1)).getIotDataEndpoint();
        verify(mockDeviceConfiguration, times(1)).getAWSRegion();
        verify(mockDeviceConfiguration, times(1)).getPrivateKeyFilePath();
        verify(mockDeviceConfiguration, times(1)).getCertificateFilePath();

        reset(mockDeviceConfiguration);
        Topic regionTopic = Topic.of(mockContext, DEVICE_PARAM_AWS_REGION, "dummyRegion");
        ccCaptor.getValue().childChanged(WhatHappened.childChanged, regionTopic);

        assertThat(clientFactory.getIotDataPlaneClient(), is(notNullValue()));

        verify(mockDeviceConfiguration, times(1)).getIotDataEndpoint();
        verify(mockDeviceConfiguration, times(1)).getAWSRegion();
        verify(mockDeviceConfiguration, times(1)).getPrivateKeyFilePath();
        verify(mockDeviceConfiguration, times(1)).getCertificateFilePath();

        reset(mockDeviceConfiguration);
        Topic endpointTopic = Topic.of(mockContext, DEVICE_PARAM_IOT_DATA_ENDPOINT, "dummyEndpoint");
        ccCaptor.getValue().childChanged(WhatHappened.childChanged, endpointTopic);

        assertThat(clientFactory.getIotDataPlaneClient(), is(notNullValue()));

        verify(mockDeviceConfiguration, times(1)).getIotDataEndpoint();
        verify(mockDeviceConfiguration, times(1)).getAWSRegion();
        verify(mockDeviceConfiguration, times(1)).getPrivateKeyFilePath();
        verify(mockDeviceConfiguration, times(1)).getCertificateFilePath();
    }
    @Test
    void GIVEN_bad_config_WHEN_configure_client_THEN_does_not_reconfigure_iot_data_client() {
        clientFactory = new IotDataPlaneClientFactory(mockDeviceConfiguration);
        assertThat(clientFactory.getIotDataPlaneClient(), is(notNullValue()));

        verify(mockDeviceConfiguration, times(1)).getIotDataEndpoint();
        verify(mockDeviceConfiguration, times(1)).getAWSRegion();
        verify(mockDeviceConfiguration, times(1)).getPrivateKeyFilePath();
        verify(mockDeviceConfiguration, times(1)).getCertificateFilePath();

        reset(mockDeviceConfiguration);
        Topic regionTopic = Topic.of(mockContext, DEVICE_PARAM_AWS_REGION, "");
        ccCaptor.getValue().childChanged(WhatHappened.childChanged, regionTopic);

        assertThat(clientFactory.getIotDataPlaneClient(), is(notNullValue()));

        verify(mockDeviceConfiguration, never()).getIotDataEndpoint();
        verify(mockDeviceConfiguration, never()).getAWSRegion();
        verify(mockDeviceConfiguration, never()).getPrivateKeyFilePath();
        verify(mockDeviceConfiguration, never()).getCertificateFilePath();

        reset(mockDeviceConfiguration);
        Topic endpointTopic = Topic.of(mockContext, DEVICE_PARAM_IOT_DATA_ENDPOINT, "");
        ccCaptor.getValue().childChanged(WhatHappened.childChanged, endpointTopic);

        assertThat(clientFactory.getIotDataPlaneClient(), is(notNullValue()));

        verify(mockDeviceConfiguration, never()).getIotDataEndpoint();
        verify(mockDeviceConfiguration, never()).getAWSRegion();
        verify(mockDeviceConfiguration, never()).getPrivateKeyFilePath();
        verify(mockDeviceConfiguration, never()).getCertificateFilePath();
    }
}
