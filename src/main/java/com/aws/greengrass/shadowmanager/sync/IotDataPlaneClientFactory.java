/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync;

import com.aws.greengrass.componentmanager.ClientConfigurationUtils;
import com.aws.greengrass.config.Node;
import com.aws.greengrass.deployment.DeviceConfiguration;
import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.shadowmanager.exception.IoTDataPlaneClientCreationException;
import com.aws.greengrass.util.Coerce;
import com.aws.greengrass.util.RetryUtils;
import com.aws.greengrass.util.Utils;
import com.aws.greengrass.util.exceptions.TLSAuthException;
import lombok.Getter;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.BackoffStrategy;
import software.amazon.awssdk.core.retry.conditions.OrRetryCondition;
import software.amazon.awssdk.core.retry.conditions.RetryCondition;
import software.amazon.awssdk.core.retry.conditions.RetryOnExceptionsCondition;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.iot.model.InternalException;
import software.amazon.awssdk.services.iot.model.InternalFailureException;
import software.amazon.awssdk.services.iot.model.LimitExceededException;
import software.amazon.awssdk.services.iot.model.ThrottlingException;
import software.amazon.awssdk.services.iotdataplane.IotDataPlaneClient;
import software.amazon.awssdk.services.iotdataplane.IotDataPlaneClientBuilder;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import javax.inject.Inject;

import static com.aws.greengrass.deployment.DeviceConfiguration.DEVICE_PARAM_AWS_REGION;
import static com.aws.greengrass.deployment.DeviceConfiguration.DEVICE_PARAM_CERTIFICATE_FILE_PATH;
import static com.aws.greengrass.deployment.DeviceConfiguration.DEVICE_PARAM_IOT_DATA_ENDPOINT;
import static com.aws.greengrass.deployment.DeviceConfiguration.DEVICE_PARAM_PRIVATE_KEY_PATH;
import static com.aws.greengrass.deployment.DeviceConfiguration.DEVICE_PARAM_ROOT_CA_PATH;

/**
 * Class to handle IoT data plane client.
 */
@Getter
@SuppressWarnings("PMD.ConfusingTernary")
public class IotDataPlaneClientFactory {
    private static final String IOT_CORE_DATA_PLANE_ENDPOINT_FORMAT = "https://%s";
    private static final Logger logger = LogManager.getLogger(IotDataPlaneClientFactory.class);
    private final AtomicReference<IotDataPlaneClient> iotDataPlaneClient = new AtomicReference<>();
    private static final Set<Class<? extends Exception>> retryableIoTExceptions = new HashSet<>(
            Arrays.asList(ThrottlingException.class, InternalException.class, InternalFailureException.class,
                    LimitExceededException.class));
    private final DeviceConfiguration deviceConfiguration;

    /**
     * Constructor for IotDataPlaneClientFactory to maintain IoT Data plane client.
     *
     * @param deviceConfiguration Device configuration class.
     */
    @Inject
    @SuppressWarnings({"PMD.NullAssignment"})
    public IotDataPlaneClientFactory(DeviceConfiguration deviceConfiguration) {
        this.deviceConfiguration = deviceConfiguration;
        deviceConfiguration.onAnyChange((what, node) -> {
            if (validString(node, DEVICE_PARAM_AWS_REGION) || validPath(node, DEVICE_PARAM_ROOT_CA_PATH) || validPath(
                    node, DEVICE_PARAM_CERTIFICATE_FILE_PATH) || validPath(node, DEVICE_PARAM_PRIVATE_KEY_PATH)
                    || validString(node, DEVICE_PARAM_IOT_DATA_ENDPOINT)) {
                iotDataPlaneClient.set(null);
            }
        });
    }

    private boolean validString(Node node, String key) {
        return node != null && node.childOf(key) && Utils.isNotEmpty(Coerce.toString(node));
    }

    /**
     * Get Iot Core Data Plane Endpoint.
     *
     * @param iotDataEndpoint the data endpoint without scheme.
     * @return Iot Data Plane Endpoint
     */
    private String getIotCoreDataPlaneEndpoint(String iotDataEndpoint) {
        return String.format(IOT_CORE_DATA_PLANE_ENDPOINT_FORMAT, iotDataEndpoint);
    }


    private boolean validPath(Node node, String key) {
        return validString(node, key) && Files.exists(Paths.get(key));
    }

    @SuppressWarnings({"PMD.AvoidCatchingGenericException"})
    private void configureClient() throws IoTDataPlaneClientCreationException {
        if (iotDataPlaneClient.get() != null) {
            return;
        }
        // To ensure that the http client is configured with mTLS, wait for the crypto key provider service (pkcs11)
        // to load. If the service is not loaded even after retrying, we throw an exception.
        try {
            waitForCryptoKeyServiceProvider();
        } catch (Exception e) {
            throw new IoTDataPlaneClientCreationException(e);
        }
        Set<Class<? extends Exception>> allExceptionsToRetryOn = new HashSet<>(retryableIoTExceptions);
        RetryCondition retryCondition = OrRetryCondition.create(RetryCondition.defaultRetryCondition(),
                RetryOnExceptionsCondition.create(allExceptionsToRetryOn));
        RetryPolicy retryPolicy = RetryPolicy.builder().numRetries(5)
                .backoffStrategy(BackoffStrategy.defaultThrottlingStrategy()).retryCondition(retryCondition).build();

        ApacheHttpClient.Builder httpClient = ClientConfigurationUtils.getConfiguredClientBuilder(deviceConfiguration);
        IotDataPlaneClientBuilder iotDataPlaneClientBuilder = IotDataPlaneClient.builder()
                // Use an empty credential provider because our requests don't need SigV4
                // signing, as they are going through IoT Core instead
                .credentialsProvider(AnonymousCredentialsProvider.create())
                .httpClient(httpClient.build())
                .overrideConfiguration(ClientOverrideConfiguration.builder().retryPolicy(retryPolicy).build());
        String region = Coerce.toString(deviceConfiguration.getAWSRegion());
        String iotDataEndpoint = Coerce.toString(deviceConfiguration.getIotDataEndpoint());
        // Region and endpoint are both required when updating endpoint config
        logger.atInfo("initialize-iot-data-client")
                .kv("service-endpoint", iotDataEndpoint)
                .kv("service-region", region).log();

        if (!Utils.isEmpty(region)) {
            iotDataPlaneClientBuilder.region(Region.of(region));
        }

        if (!Utils.isEmpty(iotDataEndpoint)) {
            iotDataPlaneClientBuilder.endpointOverride(URI.create(getIotCoreDataPlaneEndpoint(iotDataEndpoint)));
        }
        if (this.iotDataPlaneClient.get() != null) {
            this.iotDataPlaneClient.get().close();
        }
        this.iotDataPlaneClient.set(iotDataPlaneClientBuilder.build());
    }

    /**
     * Getter for IoT data plane client. This configures the client everytime the getter is used.
     *
     * @return iotDataPlaneClient
     * @throws IoTDataPlaneClientCreationException exception during client configuration
     */
    public IotDataPlaneClient getIotDataPlaneClient() throws IoTDataPlaneClientCreationException {
        configureClient();
        return iotDataPlaneClient.get();
    }


    /**
     * Checks if the crypto key service provider is available by getting key managers.
     *
     * @throws Exception exception during retry
     */
    @SuppressWarnings({"PMD.SignatureDeclareThrowsException"})
    public void waitForCryptoKeyServiceProvider() throws Exception {
        logger.atDebug().log("Checking if the crypto key service provider is available");
        RetryUtils.RetryConfig retryConfig =
                RetryUtils.RetryConfig.builder().initialRetryInterval(Duration.ofSeconds(2)).maxAttempt(3)
                        .retryableExceptions(Collections.singletonList(TLSAuthException.class)).build();
        // Ensures that the crypto key provider service is available as the key managers are obtained from it.
        RetryUtils.runWithRetry(retryConfig,
                deviceConfiguration::getDeviceIdentityKeyManagers, "get-key-managers", logger);
    }
}

