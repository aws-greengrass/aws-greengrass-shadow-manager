package com.aws.greengrass;

//import com.amazonaws.greengrass.e2e.testutilities.MetricEmitter;
//import com.aws.greengrass.resources.IotShadowResource;
//import com.aws.iot.eg.e2e.extensions.aws.IoTShadow;
//import com.aws.iot.eg.e2e.extensions.aws.IoTShadowSpec;
//import com.aws.iot.evergreen.utils.TestUtils;
//import com.aws.greengrass.resources.IotShadowResource;

import com.aws.greengrass.testing.model.ScenarioContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.inject.Inject;
import io.cucumber.guice.ScenarioScoped;
import io.cucumber.java.After;
import io.cucumber.java.en.When;
import lombok.extern.log4j.Log4j2;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.iot.iotshadow.IotShadowClient;
import software.amazon.awssdk.iot.iotshadow.model.ErrorResponse;
import software.amazon.awssdk.iot.iotshadow.model.UpdateShadowResponse;
//import software.amazon.awssdk.services.iotdataplane.DefaultIotDataPlaneClientBuilder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@Log4j2
@ScenarioScoped
public class ShadowSteps {
    public static final String VERSION_KEY = "version";
    private static final String CLASSIC_SHADOW = "";
//    private final IotDataPlaneClient iotDataPlaneClient;
    private final IotShadowResources iotShadowResources;
    private final ScenarioContext scenarioContext;
    private final ObjectMapper mapper = new ObjectMapper();
    private IotShadowClient iotShadowClient;
//    private IoTMqttSteps ioTMqttSteps;
//    private final Provider<PerformanceMonitoringSteps> perfStepProvider;

    private static final String METRIC_NAME_SHADOW_UPDATE_COUNTER = "shadow-cloud-update-counter";
    private static final String METRIC_NAME_SHADOW_UPDATE_ACCEPTED_COUNTER = "shadow-cloud-update-accepted-counter";
    private static final String METRIC_NAME_SHADOW_UPDATE_ACCEPTED_EXCEPTION = "shadow-cloud-update-accepted-exception";
    private static final String METRIC_NAME_SHADOW_UPDATE_REJECTED_COUNTER = "shadow-cloud-update-rejected-counter";
    private static final String METRIC_NAME_SHADOW_UPDATE_REJECTED_EXCEPTION = "shadow-cloud-update-rejected-exception";
    private static final ScheduledExecutorService EXECUTOR = Executors.newScheduledThreadPool(5);
    private static final AtomicLong SHADOW_UPDATE_COUNTER_SINCE_LAST_METRIC_EMITTED = new AtomicLong();
    private static final AtomicLong SHADOW_UPDATE_ACCEPTED_COUNTER_SINCE_LAST_METRIC_EMITTED = new AtomicLong();
    private static final AtomicLong SHADOW_UPDATE_ACCEPTED_EXCEPTION_SINCE_LAST_METRIC_EMITTED = new AtomicLong();
    private static final AtomicLong SHADOW_UPDATE_REJECTED_COUNTER_SINCE_LAST_METRIC_EMITTED = new AtomicLong();
    private static final AtomicLong SHADOW_UPDATE_REJECTED_EXCEPTION_SINCE_LAST_METRIC_EMITTED = new AtomicLong();
    private static final AtomicBoolean METRIC_EMITTER_RUNNING = new AtomicBoolean();
    private static final long METRIC_EMIT_INTERVAL_SECONDS = 60;
//    private static final double DEFAULT_TIMEOUT_SECONDS = 30.0 * TestUtils.getTimeOutMultiplier();
    private static final byte[] SHADOW_DOCUMENT_LEFT =
            "{\"state\":{\"reported\":{\"property\":\"".getBytes(StandardCharsets.UTF_8);
    private static final byte[] SHADOW_DOCUMENT_RIGHT = "\"}}}".getBytes(StandardCharsets.UTF_8);
    private static final byte SHADOW_BYTE = (byte) 'x';
    private static final String DEFAULT_TPS_STRING = "1";
    private static final String DEFAULT_SIZE_STRING = "256";

    @Inject
    public ShadowSteps(ScenarioContext scenarioContext, IotShadowResources iotShadowResources) {
        this.scenarioContext = scenarioContext;
        this.iotShadowResources = iotShadowResources;
//        this.ioTMqttSteps = ioTMqttSteps;
//        this.perfStepProvider = performanceMonitoringStepsProvider;
        String dataEndpoint = this.scenarioContext.get("IoTDataEndpoint");
//        iotDataPlaneClient = new DefaultIotDataPlaneClientBuilder()
//                .endpointOverride(URI.create("https://" + dataEndpoint)).build();
//        iotShadowResource.setIoTDataplaneClient(iotDataPlaneClient);
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
//    @When("I subscribe to cloud update for {word} shadows with prefix {word} with {word} things with prefix name "
//            + "{word} and emit metrics")
//    public void subscribeToShadowUpdateAndEmitMetrics2(final String noOfNamedShadowsStr, final String shadowNamePrefix,
//                                                       final String noOfThingsStr, final String thingNamePrefix)
//            throws IOException, ExecutionException, InterruptedException, TimeoutException {
//        if (!ioTMqttSteps.isSessionConnected()) {
//            ioTMqttSteps.connectMqttClient();
//        }
//        assertTrue(ioTMqttSteps.isSessionConnected());
//        MqttClientConnection connection = ioTMqttSteps.getConnection();
//        iotShadowClient = new IotShadowClient(connection);
//
//        String thingName = this.scenarioContextManager.getStringFromContext(thingNamePrefix);
//        String shadowName = this.scenarioContextManager.getStringFromContext(shadowNamePrefix);
//
//        long noOfNamedShadows = Integer.parseInt(scenarioContextManager.applyInline(noOfNamedShadowsStr));
//        long noOfThings = Integer.parseInt(scenarioContextManager.applyInline(noOfThingsStr));
//
//        for (int i = 0; i < noOfThings; i++) {
//            String actualThingName = thingName;
//            if (noOfThings > 1) {
//                actualThingName = thingName + i;
//            }
//            for (int j = 0; j < noOfNamedShadows; j++) {
//                String actualShadowName = shadowName + j;
//                subscribeToShadowUpdates(actualThingName, actualShadowName);
//                TimeUnit.MILLISECONDS.sleep(1000);
//            }
//        }
//    }

//    @When("I subscribe to cloud update for shadow {word} with thing name {word} and emit metrics")
//    public void subscribeToShadowUpdateAndEmitMetrics(final String shadowName, final String thingName)
//            throws InterruptedException, IOException, ExecutionException, TimeoutException {
//        if (!ioTMqttSteps.isSessionConnected()) {
//            ioTMqttSteps.connectMqttClient();
//        }
//        assertTrue(ioTMqttSteps.isSessionConnected());
//        MqttClientConnection connection = ioTMqttSteps.getConnection();
//        iotShadowClient = new IotShadowClient(connection);
//
//        String actualThingName = this.scenarioContextManager.getStringFromContext(thingName);
//        String actualShadowName = this.scenarioContextManager.getStringFromContext(shadowName);
//
//        subscribeToShadowUpdates(actualThingName, actualShadowName);
//    }

//    private void subscribeToShadowUpdates(String actualThingName, String actualShadowName)
//            throws InterruptedException, ExecutionException, TimeoutException {
//        UpdateNamedShadowSubscriptionRequest updateNamedShadowSubscriptionRequest =
//                new UpdateNamedShadowSubscriptionRequest();
//        updateNamedShadowSubscriptionRequest.shadowName = actualShadowName;
//        updateNamedShadowSubscriptionRequest.thingName = actualThingName;
//        iotShadowClient.SubscribeToUpdateNamedShadowAccepted(updateNamedShadowSubscriptionRequest,
//                QualityOfService.AT_MOST_ONCE, this::shadowUpdateAcceptedHandler,
//                this::shadowUpdateAcceptedExceptionHandler).get((long) DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
//        log.info("Subscribed to update deployment shadow accepted topic. Thing Name: " + actualThingName
//                + ". Shadow Name: " + actualShadowName);
//
//        iotShadowClient.SubscribeToUpdateNamedShadowRejected(updateNamedShadowSubscriptionRequest,
//                QualityOfService.AT_MOST_ONCE, this::shadowUpdateRejectedHandler,
//                this::shadowUpdateRejectedExceptionHandler).get((long) DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
//        log.info("Subscribed to update deployment shadow rejected topic. Thing Name: " + actualThingName
//                + ". Shadow Name: " + actualShadowName);
//
//        if (!METRIC_EMITTER_RUNNING.getAndSet(true)) {
//            EXECUTOR.scheduleAtFixedRate(() -> emitMetrics(perfStepProvider.get().getMetricEmitter()),
//                    METRIC_EMIT_INTERVAL_SECONDS, METRIC_EMIT_INTERVAL_SECONDS, TimeUnit.SECONDS);
//        }
//    }

    private void shadowUpdateAcceptedHandler(UpdateShadowResponse response) {
        log.info("deployment shadow update accepted");
        SHADOW_UPDATE_ACCEPTED_COUNTER_SINCE_LAST_METRIC_EMITTED.incrementAndGet();
    }

//    private void shadowUpdateAcceptedExceptionHandler(Exception e) {
//        log.atError().log("Error processing shadow update: {}", ExceptionUtils.getStackTrace(e));
//        SHADOW_UPDATE_ACCEPTED_EXCEPTION_SINCE_LAST_METRIC_EMITTED.incrementAndGet();
//    }

    private void shadowUpdateRejectedHandler(ErrorResponse errorResponse) {
        log.info("deployment shadow update rejected with error {}", errorResponse.message);
        SHADOW_UPDATE_REJECTED_COUNTER_SINCE_LAST_METRIC_EMITTED.incrementAndGet();
    }

//    private void shadowUpdateRejectedExceptionHandler(Exception e) {
//        log.atError().log("Error processing shadow update: {}", ExceptionUtils.getStackTrace(e));
//        SHADOW_UPDATE_REJECTED_EXCEPTION_SINCE_LAST_METRIC_EMITTED.incrementAndGet();
//    }
//
//    private void emitMetrics(MetricEmitter metricEmitter) {
//        long updateSuccessCount = SHADOW_UPDATE_COUNTER_SINCE_LAST_METRIC_EMITTED.getAndSet(0);
//        long updateAcceptedSuccessCount = SHADOW_UPDATE_ACCEPTED_COUNTER_SINCE_LAST_METRIC_EMITTED.getAndSet(0);
//        long updateAcceptedExceptionCount = SHADOW_UPDATE_ACCEPTED_EXCEPTION_SINCE_LAST_METRIC_EMITTED.getAndSet(0);
//        long updateRejectedSuccessCount = SHADOW_UPDATE_REJECTED_COUNTER_SINCE_LAST_METRIC_EMITTED.getAndSet(0);
//        long updateRejectedExceptionCount = SHADOW_UPDATE_REJECTED_EXCEPTION_SINCE_LAST_METRIC_EMITTED.getAndSet(0);
//
//        try {
//            if (updateSuccessCount > 0) {
//                metricEmitter.emitCounter(METRIC_NAME_SHADOW_UPDATE_COUNTER, updateSuccessCount);
//            }
//            if (updateAcceptedSuccessCount > 0) {
//                metricEmitter.emitCounter(METRIC_NAME_SHADOW_UPDATE_ACCEPTED_COUNTER, updateAcceptedSuccessCount);
//            }
//            if (updateAcceptedExceptionCount > 0) {
//                metricEmitter.emitCounter(METRIC_NAME_SHADOW_UPDATE_ACCEPTED_EXCEPTION, updateAcceptedExceptionCount);
//            }
//            if (updateRejectedSuccessCount > 0) {
//                metricEmitter.emitCounter(METRIC_NAME_SHADOW_UPDATE_REJECTED_COUNTER, updateRejectedSuccessCount);
//            }
//            if (updateRejectedExceptionCount > 0) {
//                metricEmitter.emitCounter(METRIC_NAME_SHADOW_UPDATE_REJECTED_EXCEPTION, updateRejectedExceptionCount);
//            }
//        } catch (IOException ioe) {
//            log.error("Failed to emit metric", ioe);
//        }
//    }

    @When("I can update cloud shadow for {word} with name {word} and size {word} bytes at {word} TPS")
    public void updateShadowAtTPS(final String thingName, final String shadowName, final String sizeVariableName,
                                  final String tpsVariableName) throws IOException, InterruptedException {
        String tpsString = (scenarioContext.get(tpsVariableName) == null) ? DEFAULT_TPS_STRING
                : scenarioContext.get(tpsVariableName);
        int tps = Integer.parseInt(tpsString);
        String sizeString =
                (scenarioContext.get(sizeVariableName) == null) ? DEFAULT_SIZE_STRING
                        : scenarioContext.get(sizeVariableName);
        int size = Integer.parseInt(sizeString);
        byte[] stateBytes = createShadowPayloadOfSize(size);
        log.debug("Updating shadow at TPS {} with state {}", tps, new String(stateBytes));
        if (tps > 0) {
            int intervalMs = 1000 / tps;
            EXECUTOR.scheduleAtFixedRate(() -> createShadow(thingName, shadowName, stateBytes), 0, intervalMs,
                    TimeUnit.MILLISECONDS);
        } else {
            // without tps control
            while (!Thread.currentThread().isInterrupted()) {
                createShadow(thingName, shadowName, stateBytes);
            }
        }
    }

    private void createShadow(final String thingName, final String shadowName, byte[] payload) {
        String actualThingName = this.scenarioContext.get(thingName);
        String actualShadowName = this.scenarioContext.get(shadowName);
        SdkBytes sdkBytes;
        sdkBytes = SdkBytes.fromByteArray(updateTimestamp(payload));
        try {
//            iotDataPlaneClient.updateThingShadow(
//                    UpdateThingShadowRequest.builder().thingName(actualThingName).shadowName(actualShadowName)
//                            .payload(sdkBytes).build());
//            SHADOW_UPDATE_COUNTER_SINCE_LAST_METRIC_EMITTED.incrementAndGet();
        } catch (Exception e) {
            log.error("Failed to update cloud shadow");
        }
    }
//
//    @When("I can create cloud shadow for {word} with state {word}")
//    public void createShadow(final String thingName, final String stateString) {
//        createShadow(thingName, CLASSIC_SHADOW, stateString);
//    }

//    @When("I can create cloud shadow for {word} with name {word} with state {word}")
//    public void createShadow(final String thingName, final String shadowName, final String stateString) {
//        String actualThingName = this.scenarioContextManager.getStringFromContext(thingName);
//        String actualShadowName = this.scenarioContextManager.getStringFromContext(shadowName);
//
//        iotShadowResource.getSpecs().getShadowSpecs()
//                .add(IoTShadowSpec.builder().thingName(actualThingName).shadowName(actualShadowName)
//                        .initialPayload(stateString.getBytes(Charset.defaultCharset()))
//                        .build());
//        iotShadowResource.create();
//    }

    // Create a byte array of desired size with "x" padding
    byte[] createShadowPayloadOfSize(int payloadSizeBytes) {
        byte[] shadowDocumentPayload = new byte[payloadSizeBytes];
        Arrays.fill(shadowDocumentPayload, SHADOW_BYTE);
        System.arraycopy(SHADOW_DOCUMENT_LEFT, 0, shadowDocumentPayload, 0, SHADOW_DOCUMENT_LEFT.length);
        System.arraycopy(SHADOW_DOCUMENT_RIGHT, 0, shadowDocumentPayload,
                payloadSizeBytes - SHADOW_DOCUMENT_RIGHT.length, SHADOW_DOCUMENT_RIGHT.length);
        return shadowDocumentPayload;
    }

    // Update timestamp in payload
    byte[] updateTimestamp(byte[] shadowDocumentPayload) {
        long timestamp = TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis());
        String timestampString = String.valueOf(timestamp);
        System.arraycopy(timestampString.getBytes(StandardCharsets.UTF_8), 0, shadowDocumentPayload,
                SHADOW_DOCUMENT_LEFT.length, Integer.min(timestampString.length(),
                        (shadowDocumentPayload.length - SHADOW_DOCUMENT_LEFT.length - SHADOW_DOCUMENT_RIGHT.length)));
        return shadowDocumentPayload;
    }
//
//
//    @Then("I can not get cloud shadow for {word} with name {word} within {int} seconds")
//    public void cannotGetShadow(final String thingName, final String shadowName,
//                                final int timeoutSeconds) throws IOException, InterruptedException {
//        getShadow(thingName, shadowName, null, timeoutSeconds, true, 0L);
//    }
//
//    @Then("I can get cloud shadow for {word} with name {word} with state {word} within {int} seconds")
//    public void canGetShadow(final String thingName, final String shadowName, final String stateString,
//                             final int timeoutSeconds) throws IOException, InterruptedException {
//        getShadow(thingName, shadowName, stateString, timeoutSeconds, false, 1L);
//    }
//
//    @Then("I can get cloud shadow for {word} with name {word} with version {int} and state {word} within {int} seconds")
//    public void canGetShadow(final String thingName, final String shadowName, final int version,
//                             final String stateString, final int timeoutSeconds)
//            throws IOException, InterruptedException {
//        getShadow(thingName, shadowName, stateString, timeoutSeconds, false, version);
//    }
//
//    @Then("I can not get cloud shadow for {word} within {int} seconds")
//    public void cannotGetShadow(final String thingName, final int timeoutSeconds)
//            throws IOException, InterruptedException {
//        getShadow(thingName, CLASSIC_SHADOW, null, timeoutSeconds, true, 0L);
//    }
//
//    @Then("I can get cloud shadow for {word} with state {word} within {int} seconds")
//    public void canGetShadow(final String thingName, final String stateString,
//                             final int timeoutSeconds) throws IOException, InterruptedException {
//        getShadow(thingName, CLASSIC_SHADOW, stateString, timeoutSeconds, false, 1L);
//    }
//
//    @Then("I can get cloud shadow for {word} with version {int} and state {word} within {int} seconds")
//    public void canGetShadow(final String thingName, final int version, final String stateString,
//                             final int timeoutSeconds) throws IOException, InterruptedException {
//        getShadow(thingName, CLASSIC_SHADOW, stateString, timeoutSeconds, false, version);
//    }

//    @When("I delete cloud shadow for {word}")
//    public void deleteShadow(final String thingName) {
//        deleteShadow(thingName, CLASSIC_SHADOW);
//    }

//    @When("I delete cloud shadow for {word} with name {word}")
//    public void deleteShadow(final String thingName, final String shadowName) {
//        String actualThingName = this.scenarioContextManager.getStringFromContext(thingName);
//        String actualShadowName = this.scenarioContextManager.getStringFromContext(shadowName);
//        DeleteThingShadowResponse response = iotDataPlaneClient.deleteThingShadow(DeleteThingShadowRequest.builder()
//                .shadowName(actualShadowName).thingName(actualThingName).build());
//        assertThat(response, is(notNullValue()));
//    }
//
//    private void getShadow(final String thingName, final String shadowName, final String stateString,
//                           final int timeoutSeconds, final boolean shouldNotExist, final long version)
//            throws IOException, InterruptedException {
//        AtomicReference<GetThingShadowResponse> receivedResponse = new AtomicReference<>();
//        String actualThingName = this.scenarioContextManager.getStringFromContext(thingName);
//        AtomicReference<String> actualShadowName = new AtomicReference<>(CLASSIC_SHADOW);
//        if (!CLASSIC_SHADOW.equals(shadowName)) {
//            actualShadowName.set(this.scenarioContextManager.getStringFromContext(shadowName));
//        }
//
//        boolean successful = TestUtils.eventuallyTrue((Void) -> {
//            try {
//                GetThingShadowResponse response = iotDataPlaneClient.getThingShadow(GetThingShadowRequest.builder()
//                        .thingName(actualThingName).shadowName(actualShadowName.get()).build());
//                if (response.payload() == null && shouldNotExist) {
//                    return true;
//                }
//                if (response.payload() == null) {
//                    return false;
//                }
//                receivedResponse.set(response);
//                log.debug("Received shadow response for {}/{} {}", thingName, shadowName,
//                        response.payload().asUtf8String());
//                if (shouldNotExist) {
//                    log.warn("Shadow should not exist");
//                }
//
//                // we don't consider it successful if a shadow is present that we think should not exist
//                return !shouldNotExist;
//            } catch (ResourceNotFoundException e) {
//                receivedResponse.set(null);
//                return shouldNotExist;
//            }
//        }, TimeUnit.SECONDS.toMillis((long) Math.ceil(TestUtils.getTimeOutMultiplier() * timeoutSeconds)));
//        if (!successful) {
//            if (shouldNotExist) {
//                fail("Received shadow that should not exist");
//            } else {
//                fail("Unable to get shadow");
//            }
//            return;
//        } else if (shouldNotExist && (receivedResponse.get() == null || receivedResponse.get().payload() == null)) {
//            // If we do not want to get the shadow, then this should not be successful.
//            return;
//        }

//        assertThat(receivedResponse.get().payload(), is(notNullValue()));
//        JsonNode expectedStateNode = mapper.readTree(stateString);
//        JsonNode actualStateNode = mapper.readTree(receivedResponse.get().payload().asByteArray());
//        removeTimeStamp(actualStateNode);
//        removeMetadata(actualStateNode);
//        assertThat(actualStateNode.get(VERSION_KEY).asLong(), is(version));
//        removeVersion(actualStateNode);
//        assertThat(actualStateNode, is(expectedStateNode));
//
//    }

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
