/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass;

import com.aws.greengrass.testing.model.ScenarioContext;
import com.google.inject.Inject;
import io.cucumber.guice.ScenarioScoped;
import io.cucumber.java.After;
import io.cucumber.java.en.When;
import lombok.extern.log4j.Log4j2;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

@Log4j2
@ScenarioScoped
public class ShadowSteps {
    private final IotShadowResources iotShadowResources;
    private final ScenarioContext scenarioContext;

    private static final ScheduledExecutorService EXECUTOR = Executors.newScheduledThreadPool(5);

    @Inject
    public ShadowSteps(ScenarioContext scenarioContext, IotShadowResources iotShadowResources) {
        this.scenarioContext = scenarioContext;
        this.iotShadowResources = iotShadowResources;
    }

    @After
    public void close() {
        EXECUTOR.shutdownNow();
    }

    @When("I add random shadow for {word} with name {word} in context")
    public void addShadow(final String thingName, final String shadowName) {
        String actualThingName = thingName + randomName();
        scenarioContext.put(thingName, actualThingName);
        // Reducing shadow name since we might need extra space when syncing more than 1 shadow.
        String actualShadowName = (shadowName + randomName()).substring(0, 60);
        scenarioContext.put(shadowName, actualShadowName);
        iotShadowResources.getResources().getIoTShadows().add(new IoTShadow(actualThingName, actualShadowName));
    }

    public static String randomName() {
        return String.format("e2e-%d-%s", System.currentTimeMillis(), UUID.randomUUID().toString());
    }
}
