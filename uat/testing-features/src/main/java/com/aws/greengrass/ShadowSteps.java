/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass;

import com.aws.greengrass.componentmanager.ClientConfigurationUtils;
import com.aws.greengrass.deployment.DeviceConfiguration;
import com.aws.greengrass.testing.model.ScenarioContext;
import com.aws.greengrass.util.Coerce;
import com.aws.greengrass.util.Utils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.inject.Inject;
import io.cucumber.guice.ScenarioScoped;
import io.cucumber.java.After;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import lombok.extern.log4j.Log4j2;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.iot.IotClient;
import software.amazon.awssdk.services.iot.model.DescribeEndpointRequest;
import software.amazon.awssdk.services.iotdataplane.IotDataPlaneClient;
import software.amazon.awssdk.services.iotdataplane.IotDataPlaneClientBuilder;
import software.amazon.awssdk.services.iotdataplane.model.GetThingShadowRequest;
import software.amazon.awssdk.services.iotdataplane.model.GetThingShadowResponse;
import software.amazon.awssdk.services.iotdataplane.model.ResourceNotFoundException;

import java.io.IOException;
import java.net.URI;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.fail;

import com.aws.greengrass.deployment.DeviceConfiguration;
@Log4j2
@ScenarioScoped
public class ShadowSteps {
    public static final String VERSION_KEY = "version";
    private static final String CLASSIC_SHADOW = "";
    private static final String IOT_CORE_DATA_PLANE_ENDPOINT_FORMAT = "https://%s";
    private final IotShadowResources iotShadowResources;
    private final ScenarioContext scenarioContext;

    private final ObjectMapper mapper = new ObjectMapper();
    private static final ScheduledExecutorService EXECUTOR = Executors.newScheduledThreadPool(5);
    private final IotDataPlaneClient iotDataPlaneClient;

    @Inject
    public ShadowSteps(ScenarioContext scenarioContext, IotShadowResources iotShadowResources, DeviceConfiguration deviceConfiguration) {
        this.scenarioContext = scenarioContext;
        this.iotShadowResources = iotShadowResources;

        IotClient iotClient = IotClient.builder().credentialsProvider(DefaultCredentialsProvider.create()).region(Region.US_EAST_1).build();
        String dataEndpoint = iotClient
                .describeEndpoint(DescribeEndpointRequest.builder().endpointType("iot:Data-ATS").build())
                .endpointAddress();
        iotDataPlaneClient = IotDataPlaneClient.builder()
                .endpointOverride(URI.create("https://" + dataEndpoint))
                .build();
    }

    @After
    public void close() {
        EXECUTOR.shutdownNow();
    }

    /**
     * step for adding a random shadow
     *
     * @param thingName name of thing
     * @param shadowName name of shadow
     */
    @When("I add random shadow for {word} with name {word} in context")
    public void addShadow(final String thingName, final String shadowName) {
        String actualThingName = thingName + randomName();
        scenarioContext.put(thingName, actualThingName);
        // Reducing shadow name since we might need extra space when syncing more than 1 shadow.
        String actualShadowName = (shadowName + randomName()).substring(0, 60);
        scenarioContext.put(shadowName, actualShadowName);
        iotShadowResources.getResources().getIoTShadows().add(new IoTShadow(actualThingName, actualShadowName));
    }

    /**
     * step for I can get cloud shadow for {word} with name {word} with state {word} within {int} seconds
     *
     * @param thingName     name of thing
     * @param shadowName    name of shadow
     * @param stateString   state
     * @param timeoutSeconds seconds for time to be out date
     * @throws IOException
     * @throws InterruptedException
     */
    @Then("I can get cloud shadow for {word} with name {word} with state {word} within {int} seconds")
    public void canGetShadow(final String thingName, final String shadowName, final String stateString,
                             final int timeoutSeconds) throws IOException, InterruptedException {
        getShadow(thingName, shadowName, stateString, timeoutSeconds, false, 1L);
    }

    /**
     * step for I can not get cloud shadow for {word} with name {word} within {int} seconds
     *
     * @param thingName name of thing
     * @param shadowName    name of shadow
     * @param timeoutSeconds seconds for time to be out date.
     * @throws IOException
     * @throws InterruptedException
     */
    @Then("I can not get cloud shadow for {word} with name {word} within {int} seconds")
    public void cannotGetShadow(final String thingName, final String shadowName,
                                final int timeoutSeconds) throws IOException, InterruptedException {
        getShadow(thingName, shadowName, null, timeoutSeconds, true, 0L);
    }
    public static String randomName() {
        return String.format("e2e-%d-%s", System.currentTimeMillis(), UUID.randomUUID().toString());
    }

    private void getShadow(final String thingName, final String shadowName, final String stateString,
                           final int timeoutSeconds, final boolean shouldNotExist, final long version)
            throws IOException, InterruptedException {
        AtomicReference<GetThingShadowResponse> receivedResponse = new AtomicReference<>();
        String actualThingName = this.scenarioContext.get(thingName);
        AtomicReference<String> actualShadowName = new AtomicReference<>(CLASSIC_SHADOW);
        if (!CLASSIC_SHADOW.equals(shadowName)) {
            actualShadowName.set(this.scenarioContext.get(shadowName));
        }

        boolean successful = TestUtils.eventuallyTrue((Void) -> {
            try {
                GetThingShadowResponse response = iotDataPlaneClient.getThingShadow(GetThingShadowRequest.builder()
                        .thingName(actualThingName).shadowName(actualShadowName.get()).build());
                if (response.payload() == null && shouldNotExist) {
                    return true;
                }
                if (response.payload() == null) {
                    return false;
                }
                receivedResponse.set(response);
                log.debug("Received shadow response for {}/{} {}", thingName, shadowName,
                        response.payload().asUtf8String());
                if (shouldNotExist) {
                    log.warn("Shadow should not exist");
                }

                // we don't consider it successful if a shadow is present that we think should not exist
                return !shouldNotExist;
            } catch (ResourceNotFoundException e) {
                receivedResponse.set(null);
                return shouldNotExist;
            }
        }, TimeUnit.SECONDS.toMillis((long) Math.ceil(TestUtils.getTimeOutMultiplier() * timeoutSeconds)));
        if (!successful) {
            if (shouldNotExist) {
                fail("Received shadow that should not exist");
            } else {
                fail("Unable to get shadow");
            }
            return;
        } else if (shouldNotExist && (receivedResponse.get() == null || receivedResponse.get().payload() == null)) {
            // If we do not want to get the shadow, then this should not be successful.
            return;
        }

        assertThat(receivedResponse.get().payload(), is(notNullValue()));
        JsonNode expectedStateNode = mapper.readTree(stateString);
        JsonNode actualStateNode = mapper.readTree(receivedResponse.get().payload().asByteArray());
        removeTimeStamp(actualStateNode);
        removeMetadata(actualStateNode);
        assertThat(actualStateNode.get(VERSION_KEY).asLong(), is(version));
        removeVersion(actualStateNode);
        assertThat(actualStateNode, is(expectedStateNode));
    }

    void removeVersion(JsonNode node) {
        ((ObjectNode) node).remove(VERSION_KEY);
    }

    void removeTimeStamp(JsonNode node) {
        ((ObjectNode) node).remove("timestamp");
    }

    void removeMetadata(JsonNode node) {
        ((ObjectNode) node).remove("metadata");
    }
}
