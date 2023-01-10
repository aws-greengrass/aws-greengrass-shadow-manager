/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass;

import com.aws.greengrass.testing.features.WaitSteps;
import com.aws.greengrass.testing.model.ScenarioContext;
import com.aws.greengrass.testing.resources.AWSResources;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.cucumber.guice.ScenarioScoped;
import io.cucumber.java.After;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import lombok.extern.log4j.Log4j2;
import software.amazon.awssdk.services.iotdataplane.IotDataPlaneClient;
import software.amazon.awssdk.services.iotdataplane.model.GetThingShadowRequest;
import software.amazon.awssdk.services.iotdataplane.model.GetThingShadowResponse;
import software.amazon.awssdk.services.iotdataplane.model.ResourceNotFoundException;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.fail;

@Log4j2
@ScenarioScoped
public class ShadowSteps {
    public static final String VERSION_KEY = "version";
    private static final String CLASSIC_SHADOW = "";
    private final ScenarioContext scenarioContext;
    private final AWSResources awsResources;
    private final ObjectMapper mapper = new ObjectMapper();
    private static final ScheduledExecutorService EXECUTOR = Executors.newScheduledThreadPool(5);
    private final Provider<IotDataPlaneClient> iotDataPlaneClientProvider;
    private final WaitSteps waits;

    @Inject
    @SuppressWarnings("MissingJavadocMethod")
    public ShadowSteps(ScenarioContext scenarioContext, AWSResources resources,
                       Provider<IotDataPlaneClient> iotDataPlaneClient, final WaitSteps waits) {
        this.scenarioContext = scenarioContext;
        this.awsResources = resources;
        this.iotDataPlaneClientProvider = iotDataPlaneClient;
        this.waits = waits;
    }

    @After
    public void close() {
        EXECUTOR.shutdownNow();
    }

    /**
     * step for adding a random shadow.
     *
     * @param thingName  name of thing
     * @param shadowName name of shadow
     */
    @When("I add random shadow for {word} with name {word} in context")
    public void addShadow(final String thingName, final String shadowName) {
        String actualThingName = thingName + randomName();
        scenarioContext.put(thingName, actualThingName);
        // Reducing shadow name since we might need extra space when syncing more than 1 shadow.
        String actualShadowName = (shadowName + randomName()).substring(0, 60);
        scenarioContext.put(shadowName, actualShadowName);
        awsResources.create(IoTShadowSpec.builder().shadowName(actualShadowName).thingName(actualThingName).build());
    }

    /**
     * step for I can get cloud shadow for {word} with name {word} with state {word} within {int} seconds.
     *
     * @param thingName     name of thing
     * @param shadowName    name of shadow
     * @param stateString   state
     * @param timeoutSeconds seconds for time to be out date
     * @throws IOException IOException
     * @throws InterruptedException InterruptedException
     */
    @Then("I can get cloud shadow for {word} with name {word} with state {word} within {int} seconds")
    public void canGetShadow(final String thingName, final String shadowName, final String stateString,
                             final int timeoutSeconds) throws IOException, InterruptedException {
        getShadow(thingName, shadowName, stateString, timeoutSeconds, false, 1L);
    }

    /**
     * step for I can not get cloud shadow for {word} with name {word} within {int} seconds.
     *
     * @param thingName      name of thing
     * @param shadowName     name of shadow
     * @param timeoutSeconds seconds for time to be out date.
     * @throws IOException          IO Exception
     * @throws InterruptedException Interrupted Exception
     */
    @Then("I can not get cloud shadow for {word} with name {word} within {int} seconds")
    public void cannotGetShadow(final String thingName, final String shadowName,
                                final int timeoutSeconds) throws IOException, InterruptedException {
        getShadow(thingName, shadowName, null, timeoutSeconds, true, 0L);
    }

    private String randomName() {
        return String.format("e2e-%d-%s", System.currentTimeMillis(), UUID.randomUUID().toString());
    }

    private void getShadow(final String thingName, final String shadowName, final String stateString,
                           final int timeoutSeconds, final boolean shouldNotExist, final long version)
            throws IOException, InterruptedException {
        AtomicReference<GetThingShadowResponse> receivedResponse = new AtomicReference<>();
        boolean successful = waits.untilTrue(() -> compute(thingName, shadowName, shouldNotExist, receivedResponse),
                timeoutSeconds, TimeUnit.SECONDS);
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
        JsonNode actualStateNode = mapper.readTree(receivedResponse.get().payload().asByteArray());
        removeTimeStamp(actualStateNode);
        removeMetadata(actualStateNode);
        removeVersion(actualStateNode);
        JsonNode expectedStateNode = mapper.readTree(stateString);
        assertThat(actualStateNode, is(expectedStateNode));
    }

    private boolean compute(String thingName, String shadowName, boolean shouldNotExist,
                            AtomicReference<GetThingShadowResponse> receivedResponse) {
        String actualThingName = this.scenarioContext.get(thingName);
        AtomicReference<String> actualShadowName = new AtomicReference<>(CLASSIC_SHADOW);
        if (!CLASSIC_SHADOW.equals(shadowName)) {
            actualShadowName.set(this.scenarioContext.get(shadowName));
        }
        try {
            GetThingShadowResponse response = iotDataPlaneClientProvider.get()
                    .getThingShadow(GetThingShadowRequest.builder()
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
