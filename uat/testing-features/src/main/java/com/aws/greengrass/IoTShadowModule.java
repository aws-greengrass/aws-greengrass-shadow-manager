/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass;

import com.aws.greengrass.testing.modules.AbstractAWSResourceModule;
import com.aws.greengrass.testing.modules.model.AWSResourcesContext;
import com.google.auto.service.AutoService;
import com.google.inject.Module;
import com.google.inject.Provides;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.services.iot.IotClient;
import software.amazon.awssdk.services.iot.model.DescribeEndpointRequest;
import software.amazon.awssdk.services.iotdataplane.IotDataPlaneClient;
import software.amazon.awssdk.services.iotdataplane.IotDataPlaneClientBuilder;

import java.net.URI;
import javax.inject.Singleton;

@AutoService(Module.class)
public class IoTShadowModule extends AbstractAWSResourceModule<IotDataPlaneClient, IoTShadowLifecycle> {
    @Singleton
    @Provides
    @Override
    protected IotDataPlaneClient providesClient(
            AwsCredentialsProvider provider,
            AWSResourcesContext context,
            ApacheHttpClient.Builder httpClientBuilder) {
        IotClient iotClient = IotClient.builder()
                .credentialsProvider(DefaultCredentialsProvider.create())
                .region(context.region())
                .build();
        String dataEndpoint = iotClient
                .describeEndpoint(DescribeEndpointRequest.builder().endpointType("iot:Data-ATS").build())
                .endpointAddress();
        IotDataPlaneClientBuilder builder = IotDataPlaneClient.builder()
                .credentialsProvider(provider)
                .httpClientBuilder(httpClientBuilder)
                .region(context.region())
                .endpointOverride(URI.create("https://" + dataEndpoint));
        if (!context.isProd()) {
            String endpoint = String.format("https://%s.%s.iot.%s",
                    context.envStage(),
                    context.region().metadata().id(),
                    context.region().metadata().domain());
            builder.endpointOverride(URI.create(endpoint));
        }
        return builder.build();
    }
}
