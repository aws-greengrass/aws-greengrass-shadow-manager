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
import software.amazon.awssdk.services.iotdataplane.IotDataPlaneClient;

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
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
    @SuppressWarnings("PMD.CloseResource")
    void GIVEN_device_configuration_WHEN_crypto_service_available_THEN_configure_iot_data_client_only_once() throws
            TLSAuthException, IoTDataPlaneClientCreationException {
        clientFactory = new IotDataPlaneClientFactory(mockDeviceConfiguration);
        IotDataPlaneClient client = clientFactory.getIotDataPlaneClient();
        assertThat(client, is(notNullValue()));
        lenient().when(mockDeviceConfiguration.getDeviceIdentityKeyManagers()).thenReturn(new KeyManager[0]);
        //Invoked only once
        verify(mockDeviceConfiguration, times(1)).getIotDataEndpoint();
        verify(mockDeviceConfiguration, times(1)).getAWSRegion();
        verify(mockDeviceConfiguration, times(1)).getDeviceIdentityKeyManagers();

        assertThat(clientFactory.getIotDataPlaneClient(), is(client));
        //Not invoked for another getter
        verify(mockDeviceConfiguration, times(1)).getIotDataEndpoint();
        verify(mockDeviceConfiguration, times(1)).getAWSRegion();
        verify(mockDeviceConfiguration, times(1)).getDeviceIdentityKeyManagers();
    }


    @Test
    void GIVEN_device_configuration_WHEN_service_not_available_THEN_do_not_configure_iot_data_client(ExtensionContext context)
            throws TLSAuthException {
        ignoreExceptionOfType(context, TLSAuthException.class);
        when(mockDeviceConfiguration.getDeviceIdentityKeyManagers()).thenThrow(TLSAuthException.class);
        clientFactory = new IotDataPlaneClientFactory(mockDeviceConfiguration);
        // Wait for the crypto key provider service from the constructor - retry 3 times
        verify(mockDeviceConfiguration, times(3)).getDeviceIdentityKeyManagers();
        IoTDataPlaneClientCreationException thrown = assertThrows(IoTDataPlaneClientCreationException.class,
                () -> clientFactory.getIotDataPlaneClient());
        assertThat(thrown.getCause(), instanceOf(TLSAuthException.class));

        verify(mockDeviceConfiguration, times(0)).getIotDataEndpoint();
        verify(mockDeviceConfiguration, times(0)).getAWSRegion();
        // Wait for the crypto key provider service again when using getter - retry 3 more times
        verify(mockDeviceConfiguration, times(6)).getDeviceIdentityKeyManagers();
    }

    @Test
    void GIVEN_device_config_WHEN_service_not_available_in_constructor_THEN_wait_for_service_again_And_if_available_then_configure_client(ExtensionContext context)
            throws TLSAuthException, IoTDataPlaneClientCreationException {
        ignoreExceptionOfType(context, TLSAuthException.class);
        when(mockDeviceConfiguration.getDeviceIdentityKeyManagers()).thenThrow(TLSAuthException.class);
        clientFactory = new IotDataPlaneClientFactory(mockDeviceConfiguration);
        // Wait for the crypto key provider service from the constructor - retry 3 times
        verify(mockDeviceConfiguration, times(3)).getDeviceIdentityKeyManagers();
        reset(mockDeviceConfiguration);
        when(mockDeviceConfiguration.getDeviceIdentityKeyManagers()).thenReturn(new KeyManager[0]);
        assertThat(clientFactory.getIotDataPlaneClient(), is(notNullValue()));

        verify(mockDeviceConfiguration, times(1)).getIotDataEndpoint();
        verify(mockDeviceConfiguration, times(1)).getAWSRegion();
        // Wait for the crypto key provider service again when using getter - retry only 1 more time as it is available
        verify(mockDeviceConfiguration, times(1)).getDeviceIdentityKeyManagers();
    }

}
