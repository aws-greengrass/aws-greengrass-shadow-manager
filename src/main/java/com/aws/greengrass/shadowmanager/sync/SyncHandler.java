/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync;

import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.shadowmanager.exception.RetryableException;
import com.aws.greengrass.shadowmanager.sync.model.CloudDeleteSyncRequest;
import com.aws.greengrass.shadowmanager.sync.model.CloudUpdateSyncRequest;
import com.aws.greengrass.shadowmanager.sync.model.FullShadowSyncRequest;
import com.aws.greengrass.shadowmanager.sync.model.LocalDeleteSyncRequest;
import com.aws.greengrass.shadowmanager.sync.model.LocalUpdateSyncRequest;
import com.aws.greengrass.shadowmanager.sync.model.SyncContext;
import com.aws.greengrass.shadowmanager.sync.model.SyncRequest;
import com.aws.greengrass.util.Pair;
import com.aws.greengrass.util.RetryUtils;
import com.fasterxml.jackson.databind.JsonNode;
import software.amazon.awssdk.aws.greengrass.model.ConflictError;
import software.amazon.awssdk.services.iotdataplane.model.ConflictException;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.inject.Inject;

import static com.aws.greengrass.shadowmanager.model.Constants.LOG_SHADOW_NAME_KEY;
import static com.aws.greengrass.shadowmanager.model.Constants.LOG_THING_NAME_KEY;
import static com.aws.greengrass.shadowmanager.model.LogEvents.SYNC;

/**
 * Handle syncing shadows between AWS IoT Device Shadow Service and the local shadow service.
 */
public class SyncHandler {
    private static final Logger logger = LogManager.getLogger(SyncHandler.class);

    /**
     * Configuration for retrying sync requests.
     */
    private static final RetryUtils.RetryConfig DEFAULT_RETRY_CONFIG =
            RetryUtils.RetryConfig.builder()
                    .maxAttempt(5)
                    .initialRetryInterval(Duration.of(3, ChronoUnit.SECONDS))
                    .maxRetryInterval(Duration.of(1, ChronoUnit.MINUTES))
                    .retryableExceptions(Collections.singletonList(RetryableException.class)).build();
    /**
     * Configuration for retrying a sync request immediately after failing with the {@link #DEFAULT_RETRY_CONFIG}.
     */
    private static final RetryUtils.RetryConfig FAILED_RETRY_CONFIG =
            RetryUtils.RetryConfig.builder()
                    .maxAttempt(3)
                    .initialRetryInterval(Duration.of(30, ChronoUnit.SECONDS))
                    .maxRetryInterval(Duration.of(2, ChronoUnit.MINUTES))
                    .retryableExceptions(Collections.singletonList(RetryableException.class))
                    .build();

    private static final String SYNC_EVENT_TYPE = SYNC.code();

    /**
     * Default number of threads to use for executing sync requests.
     */
    public static final int DEFAULT_PARALLELISM = 1;

    private final RequestBlockingQueue syncQueue;

    private final ExecutorService syncExecutorService;

    /**
     * The threads running the sync loop.
     */
    final List<Future<?>> syncThreads = new ArrayList<>();

    /**
     * Indicates whether syncing is running or not.
     */
    final AtomicBoolean syncing = new AtomicBoolean(false);

    /**
     * Lock to ensure start and stop can't happen simultaneously.
     */
    private final Object lifecycleLock = new Object();

    /**
     * Interface for executing sync requests.
     */
    private final Retryer retryer;

    /**
     * Context object containing handlers useful for sync requests.
     */
    private SyncContext context;



    /**
     * Construct a new instance.
     *
     * @param syncQueue the queue for storing sync requests
     * @param executorService provider of threads for syncing
     */
    @Inject
    public SyncHandler(RequestBlockingQueue syncQueue, ExecutorService executorService) {
        this (syncQueue, executorService,
                // retry wrapper so that requests can be mocked
                (config, request, context) ->
                        RetryUtils.runWithRetry(config,
                                () -> {
                                    request.execute(context);
                                    return null;
                                },
                                SYNC_EVENT_TYPE, logger));
    }

    SyncHandler(RequestBlockingQueue syncQueue, ExecutorService executorService,
            Retryer retryer) {
        this.syncQueue = syncQueue;
        this.syncExecutorService = executorService;
        this.retryer = retryer;
    }

    /**
     * Interface for executing sync requests with retries.
     */
    @FunctionalInterface
    interface Retryer {
        @SuppressWarnings("PMD.SignatureDeclareThrowsException")
        void run(RetryUtils.RetryConfig config, SyncRequest request, SyncContext context) throws Exception;
    }

    /**
     * Performs a full sync on all shadows. Clears any existing sync requests and will create full shadow sync requests
     * for all shadows.
     *
     * @throws InterruptedException if the thread is interrupted while enqueuing data
     */
    private void fullSyncOnAllShadows() throws InterruptedException {
        syncQueue.clear();

        List<Pair<String, String>> shadows = context.getDao().listSyncedShadows();

        if (shadows.size() > syncQueue.remainingCapacity()) {
            logger.atWarn(SYNC_EVENT_TYPE)
                    .addKeyValue("syncedShadows", shadows.size())
                    .addKeyValue("syncQueueCapacity", syncQueue.remainingCapacity())
                    .log("There are more shadows than space in the sync queue. Syncing will block");
        }
        Iterator<FullShadowSyncRequest> it =
                shadows.stream().map(p -> new FullShadowSyncRequest(p.getLeft(), p.getRight())).iterator();
        while (syncing.get() && it.hasNext()) {
            syncQueue.put(it.next());
        }
    }

    /**
     * Start sync threads to process sync requests. This automatically starts a full sync for all shadows.
     * @param context         an context object for syncing
     * @param syncParallelism number of threads to use for syncing
     */
    public void start(SyncContext context, int syncParallelism) {
        synchronized (lifecycleLock) {
            if (syncing.get()) {
                logger.atDebug(SYNC_EVENT_TYPE).log("Syncing is already in process. Ignoring request to start");
                return;
            }
            this.context = context;
            logger.atInfo(SYNC_EVENT_TYPE).log("Start syncing");
            for (int i = 0; i < syncParallelism; i++) {
                syncThreads.add(syncExecutorService.submit(this::syncLoop));
            }
            syncing.set(true);
        }

        // enqueuing full sync requests does not need to be part of lifecycle sync lock as nucleus executes lifecycle
        // actions on backing thread
        try {
            fullSyncOnAllShadows();
        } catch (InterruptedException e) {
            logger.atWarn(SYNC_EVENT_TYPE)
                    .log("Interrupted while queuing full sync requests at startup. Syncing will stop");
            stop();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Stops sync threads and clear syncing queue.
     */
    public void stop() {
        synchronized (lifecycleLock) {
            if (!syncing.get()) {
                logger.atDebug(SYNC_EVENT_TYPE).log("Syncing is already stopped. Ignoring request to stop");
                return;
            }

            logger.atInfo(SYNC_EVENT_TYPE).log("Stop syncing");
            syncing.set(false);

            logger.atDebug(SYNC_EVENT_TYPE).log("Cancel {} sync thread(s)", syncThreads.size());
            syncThreads.forEach(t -> t.cancel(true));
            syncThreads.clear();

            int remaining = syncQueue.size();
            syncQueue.clear();

            if (remaining > 0) {
                logger.atInfo(SYNC_EVENT_TYPE).log("Stopped syncing with {} pending sync items", remaining);
            }
        }
    }

    /**
     * Take and execute items from the sync queue. This is intended to be run in a separate thread.
     */
    @SuppressWarnings({"PMD.CompareObjectsWithEquals", "PMD.AvoidCatchingGenericException"})
    private void syncLoop() {
        logger.atInfo(SYNC_EVENT_TYPE).log("Start waiting for sync requests");
        try {
            SyncRequest request = syncQueue.take();
            RetryUtils.RetryConfig retryConfig = DEFAULT_RETRY_CONFIG;
            do {
                try {
                    logger.atInfo(SYNC_EVENT_TYPE)
                            .addKeyValue(LOG_THING_NAME_KEY, request.getThingName())
                            .addKeyValue(LOG_SHADOW_NAME_KEY, request.getShadowName())
                            .addKeyValue("Type", request.getClass().getSimpleName())
                            .log("Executing sync request");

                    retryer.run(retryConfig, request, context);
                    retryConfig = DEFAULT_RETRY_CONFIG; // reset the retry config back to default after success

                    logger.atDebug(SYNC_EVENT_TYPE).log("Waiting for next sync request");
                    request = syncQueue.take();
                } catch (RetryableException e) {
                    // this will be rethrown if all retries fail in RetryUtils
                    logger.atDebug(SYNC_EVENT_TYPE)
                            .cause(e)
                            .addKeyValue(LOG_THING_NAME_KEY, request.getThingName())
                            .addKeyValue(LOG_SHADOW_NAME_KEY, request.getShadowName())
                            .log("Retry sync request. Adding back to queue");

                    // put request to back of queue and get the front of queue in a single operation
                    SyncRequest failedRequest = request;
                    request = syncQueue.offerAndTake(request);

                    // if queue was empty, we are going to immediately retrying the same request. For this case don't
                    // use the default retry configuration - keep from spamming too quickly
                    if (request == failedRequest) {
                        retryConfig = FAILED_RETRY_CONFIG;
                    }
                } catch (InterruptedException e) {
                    logger.atWarn(SYNC_EVENT_TYPE).log("Interrupted while waiting for sync requests");
                    Thread.currentThread().interrupt();
                } catch (ConflictException | ConflictError e) {
                    logger.atWarn(SYNC_EVENT_TYPE)
                            .cause(e)
                            .addKeyValue(LOG_THING_NAME_KEY, request.getThingName())
                            .addKeyValue(LOG_SHADOW_NAME_KEY, request.getShadowName())
                            .log("Received conflict when processing request. Retrying as a full sync");
                    // don't need to add to queue as we want to immediately retry as a full sync
                    request = new FullShadowSyncRequest(request.getThingName(), request.getShadowName());
                } catch (Exception e) {
                    logger.atError(SYNC_EVENT_TYPE)
                            .cause(e)
                            .addKeyValue(LOG_THING_NAME_KEY, request.getThingName())
                            .addKeyValue(LOG_SHADOW_NAME_KEY, request.getShadowName())
                            .log("Skipping sync request");
                    request = syncQueue.take();
                }
            } while (!Thread.currentThread().isInterrupted());
        } catch (InterruptedException e) {
            logger.atWarn(SYNC_EVENT_TYPE).log("Interrupted while waiting for sync requests");
        }
        logger.atInfo(SYNC_EVENT_TYPE).log("Stop waiting for sync requests");
    }

    /**
     * Put a sync request into the queue if syncing is started.
     * <p/>
     * This will block if the queue is full. This is intentional as non-blocking requires either an unbounded queue
     * of requests, or an Executor service which creates threads from an unbounded queue.
     * <p/>
     * We cannot support an unbounded queue as that will lead to memory pressure - instead requests need to be
     * throttled.
     * <p/>
     * Synchronized so that there is at most only one put in progress waiting to be added if queue is full
     * @param request the request to add.
     */
    synchronized void putSyncRequest(SyncRequest request) {
        if (!syncing.get()) {
            logger.atTrace(SYNC_EVENT_TYPE)
                    .addKeyValue(LOG_THING_NAME_KEY, request.getThingName())
                    .addKeyValue(LOG_SHADOW_NAME_KEY, request.getShadowName())
                    .log("Syncing is stopped. Ignoring sync request");
            return;
        }
        try {
            syncQueue.put(request);

            // the above put call will block. If syncing is stopped while waiting but after the put call succeeds then
            // remove the request we just added
            if (!syncing.get()) {
                syncQueue.remove(request);
            }
        } catch (InterruptedException e) {
            logger.atWarn(SYNC_EVENT_TYPE)
                    .addKeyValue(LOG_THING_NAME_KEY, request.getThingName())
                    .addKeyValue(LOG_SHADOW_NAME_KEY, request.getShadowName())
                    .log("Interrupted while putting sync request into queue");
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Pushes an update sync request to the request queue to update shadow in the cloud after a local shadow has
     * been successfully updated.
     *
     * @param thingName      The thing name associated with the sync shadow update
     * @param shadowName     The shadow name associated with the sync shadow update
     * @param updateDocument The update shadow request
     */
    public void pushCloudUpdateSyncRequest(String thingName, String shadowName, JsonNode updateDocument) {
        putSyncRequest(new CloudUpdateSyncRequest(thingName, shadowName, updateDocument));
    }

    /**
     * Pushes an update sync request to the request queue to update local shadow after a cloud shadow has
     * been successfully updated.
     *
     * @param thingName      The thing name associated with the sync shadow update
     * @param shadowName     The shadow name associated with the sync shadow update
     * @param updateDocument Update document to be applied to local shadow
     */
    public void pushLocalUpdateSyncRequest(String thingName, String shadowName, byte[] updateDocument) {
        putSyncRequest(new LocalUpdateSyncRequest(thingName, shadowName, updateDocument));
    }

    /**
     * Pushes a delete sync request in the request queue to delete a shadow in the cloud after a local shadow has
     * been successfully deleted.
     *
     * @param thingName  The thing name associated with the sync shadow update
     * @param shadowName The shadow name associated with the sync shadow update
     */
    public void pushCloudDeleteSyncRequest(String thingName, String shadowName) {
        putSyncRequest(new CloudDeleteSyncRequest(thingName, shadowName));
    }

    /**
     * Pushes a delete sync request in the request queue to delete a local shadow after a cloud shadow has
     * been successfully deleted.
     *
     * @param thingName     The thing name associated with the sync shadow update
     * @param shadowName    The shadow name associated with the sync shadow update
     * @param deletePayload Delete response payload containing the deleted shadow version
     */
    public void pushLocalDeleteSyncRequest(String thingName, String shadowName, byte[] deletePayload) {
        putSyncRequest(new LocalDeleteSyncRequest(thingName, shadowName, deletePayload));
    }

    /**
     * Performs a full sync on a specific shadow.
     *
     * @param thingName  The thing name associated with the sync shadow update
     * @param shadowName The shadow name associated with the sync shadow update
     */
    public void fullSyncOnShadow(String thingName, String shadowName) {
        putSyncRequest(new FullShadowSyncRequest(thingName, shadowName));
    }
}
