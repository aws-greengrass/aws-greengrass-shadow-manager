/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync;

import com.aws.greengrass.config.Topic;
import com.aws.greengrass.dependency.Context;
import com.aws.greengrass.deployment.DeviceConfiguration;
import com.aws.greengrass.shadowmanager.exception.IoTDataPlaneClientCreationException;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.util.exceptions.TLSAuthException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;


import javax.net.ssl.KeyManager;

import static com.aws.greengrass.deployment.DeviceConfiguration.DEVICE_PARAM_AWS_REGION;
import static com.aws.greengrass.deployment.DeviceConfiguration.DEVICE_PARAM_IOT_DATA_ENDPOINT;
import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith({MockitoExtension.class, GGExtension.class})
class IotDataPlaneClientFactoryTest {

    @Mock
    private DeviceConfiguration mockDeviceConfiguration;
    @Mock
    private Context mockContext;

    private IotDataPlaneClientFactory clientFactory;

    @BeforeEach
    void setup() {
        Topic dataEndpointTopic = Topic.of(mockContext, DEVICE_PARAM_IOT_DATA_ENDPOINT, "xxxxxx-ats.iot.us-west-2.amazonaws.com");
        lenient().when(mockDeviceConfiguration.getIotDataEndpoint()).thenReturn(dataEndpointTopic);
        Topic regionTopic = Topic.of(mockContext, DEVICE_PARAM_AWS_REGION, "us-west-2");
        lenient().when(mockDeviceConfiguration.getAWSRegion()).thenReturn(regionTopic);
    }

    @Test
    void GIVEN_device_configuration_WHEN_crypto_service_available_THEN_configure_iot_data_client() throws TLSAuthException, IoTDataPlaneClientCreationException {
        clientFactory = new IotDataPlaneClientFactory(mockDeviceConfiguration);
        assertThat(clientFactory.getIotDataPlaneClient(), is(notNullValue()));
        lenient().when(mockDeviceConfiguration.getDeviceIdentityKeyManagers()).thenReturn(new KeyManager[0]);
        verify(mockDeviceConfiguration, times(1)).getIotDataEndpoint();
        verify(mockDeviceConfiguration, times(1)).getAWSRegion();
        //Invoked only once
        verify(mockDeviceConfiguration, times(1)).getDeviceIdentityKeyManagers();
    }


    @Test
    void GIVEN_device_configuration_WHEN_service_not_available_THEN_configure_iot_data_client_(ExtensionContext context)
            throws TLSAuthException, IoTDataPlaneClientCreationException {
        ignoreExceptionOfType(context, TLSAuthException.class);

        clientFactory = new IotDataPlaneClientFactory(mockDeviceConfiguration);
        lenient().when(mockDeviceConfiguration.getDeviceIdentityKeyManagers()).thenThrow(TLSAuthException.class);
        IoTDataPlaneClientCreationException thrown = assertThrows(IoTDataPlaneClientCreationException.class, () -> clientFactory.getIotDataPlaneClient());
        assertThat(thrown.getCause(), instanceOf(TLSAuthException.class));

        verify(mockDeviceConfiguration, times(0)).getIotDataEndpoint();
        verify(mockDeviceConfiguration, times(0)).getAWSRegion();
        //Retried 3 times
        verify(mockDeviceConfiguration, times(3)).getDeviceIdentityKeyManagers();
    }
}
