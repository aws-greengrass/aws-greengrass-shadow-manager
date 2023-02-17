/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync.strategy;

import com.aws.greengrass.logging.impl.config.LogConfig;
import com.aws.greengrass.shadowmanager.exception.UnknownShadowException;
import com.aws.greengrass.shadowmanager.sync.RequestBlockingQueue;
import com.aws.greengrass.shadowmanager.sync.Retryer;
import com.aws.greengrass.shadowmanager.sync.model.CloudUpdateSyncRequest;
import com.aws.greengrass.shadowmanager.sync.model.Direction;
import com.aws.greengrass.shadowmanager.sync.model.FullShadowSyncRequest;
import com.aws.greengrass.shadowmanager.sync.model.OverwriteCloudShadowRequest;
import com.aws.greengrass.shadowmanager.sync.model.OverwriteLocalShadowRequest;
import com.aws.greengrass.shadowmanager.sync.model.SyncContext;
import com.aws.greengrass.shadowmanager.sync.model.SyncRequest;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.event.Level;
import software.amazon.awssdk.aws.greengrass.model.ConflictError;
import software.amazon.awssdk.services.iotdataplane.model.ConflictException;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.time.Duration.ofSeconds;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings({"PMD.AvoidCatchingGenericException"})
@ExtendWith({MockitoExtension.class, GGExtension.class})
public abstract class SyncStrategyTestBase<T extends BaseSyncStrategy, S extends ExecutorService> {

    Retryer mockRetryer;
    SyncContext mockSyncContext;
    RequestBlockingQueue mockRequestBlockingQueue;
    FullShadowSyncRequest mockFullShadowSyncRequest;
    S executorService;
    T strategy;

    Supplier<S> executorServiceSupplier;

    SyncStrategyTestBase(Supplier<S> execServiceSupplier) {
        this.executorServiceSupplier = execServiceSupplier;
    }

    abstract T defaultTestInstance();

    @BeforeEach
    void setup() {
        executorService = executorServiceSupplier.get();
        mockRequestBlockingQueue = mock(RequestBlockingQueue.class);
        mockSyncContext = mock(SyncContext.class);
        mockRetryer = mock(Retryer.class);
        mockFullShadowSyncRequest = mock(FullShadowSyncRequest.class);
        strategy = defaultTestInstance();
    }

    @BeforeAll
    static void setupLogger() {
        LogConfig.getRootLogConfig().setLevel(Level.ERROR);
    }

    @AfterAll
    static void cleanupLogger() {
        LogConfig.getRootLogConfig().setLevel(Level.INFO);
    }

    @AfterEach
    void tearDown() {
        strategy.stop();
        executorService.shutdownNow();
    }

    @Test
    void GIVEN_sync_request_WHEN_syncing_stopped_THEN_request_added_back_without_executing(
            ExtensionContext extensionContext)
            throws Exception {

        CountDownLatch requestCalled = new CountDownLatch(1);
        when(strategy.getRequest()).thenAnswer(invocation -> {
            strategy.syncing.set(false);
            requestCalled.countDown();
            return mockFullShadowSyncRequest;
        });

        strategy.start(mockSyncContext, 1);
        if (!requestCalled.await(5, TimeUnit.SECONDS)) {
            fail("sync request not taken from queue");
        }

        verify(mockRetryer, never()).run(any(), any(), any());
        verify(mockRequestBlockingQueue, timeout(ofSeconds(5).toMillis())).offer(mockFullShadowSyncRequest);
    }

    @Test
    void GIVEN_sync_request_WHEN_syncing_stopped_after_enter_loop_THEN_request_added_back_without_executing(ExtensionContext extensionContext)
            throws Exception {
        CountDownLatch finished = new CountDownLatch(1);
        SyncRequest request2 = mock(CloudUpdateSyncRequest.class);

        when(strategy.getRequest())
                .thenReturn(mockFullShadowSyncRequest)
                .thenAnswer(i -> {
                    strategy.syncing.set(false);
                    finished.countDown();
                    return request2;
                });
        strategy.start(mockSyncContext, 1);

        if (!finished.await(10, TimeUnit.SECONDS)) {
            fail("Did not return second sync request");
        }

        verify(mockRetryer, times(1)).run(any(), eq(mockFullShadowSyncRequest), any());
        verify(mockRequestBlockingQueue, timeout(ofSeconds(5).toMillis())).offer(request2);
    }

    @Test
    void GIVEN_sync_request_WHEN_syncing_stopped_after_acquire_permit_THEN_request_added_back_without_executing(ExtensionContext extensionContext)
            throws Exception {
        Semaphore s = mock(Semaphore.class);

        // latch for when stop checks for running requests
        CountDownLatch tryAcquireCalled = new CountDownLatch(1);

        when(s.tryAcquire(1)).thenAnswer(i -> {
            tryAcquireCalled.countDown();
            return false;
        });
        doAnswer(i -> {
                    if (!tryAcquireCalled.await(5, TimeUnit.SECONDS)) {
                        fail("tryAcquire was not called");
                    }
                    return null;
                }
        ).when(s).acquire();

        SyncRequest request2 = mock(CloudUpdateSyncRequest.class);

        final Future<?>[] stopFuture = new Future<?>[1];

        when(strategy.getRequest())
                .thenAnswer(i -> mockFullShadowSyncRequest)
                .thenAnswer(i -> {
                    strategy.criticalExecBlock = s;
                    // wait for the stop to try and acquire - it will then wait until requests have finished
                    stopFuture[0] = Executors.newSingleThreadExecutor().submit(() -> strategy.stop());
                    return request2;
                });

        strategy.start(mockSyncContext, 1);

        if (!tryAcquireCalled.await(5, TimeUnit.SECONDS)) {
            fail("Did not attempt to stop while executing");
        }

        // wait for the request to finish
        try {
            stopFuture[0].get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            fail("Did not finish stopping", e);
        }
        verify(mockRetryer, times(1)).run(any(), eq(mockFullShadowSyncRequest), any());
        verify(mockRetryer, never()).run(any(), eq(request2), any());

        verify(mockRequestBlockingQueue, timeout(ofSeconds(5).toMillis()).times(1)).offer(request2);
    }

    static Stream<Arguments> expectedSyncRequestsOnConflictByDirection() {
        return Stream.of(
                arguments(Direction.CLOUD_TO_DEVICE, ConflictException.class, OverwriteLocalShadowRequest.class),
                arguments(Direction.CLOUD_TO_DEVICE, ConflictError.class, OverwriteLocalShadowRequest.class),
                arguments(Direction.CLOUD_TO_DEVICE, UnknownShadowException.class, OverwriteLocalShadowRequest.class),
                arguments(Direction.DEVICE_TO_CLOUD, ConflictException.class, OverwriteCloudShadowRequest.class),
                arguments(Direction.DEVICE_TO_CLOUD, ConflictError.class, OverwriteCloudShadowRequest.class),
                arguments(Direction.DEVICE_TO_CLOUD, UnknownShadowException.class, OverwriteCloudShadowRequest.class),
                arguments(Direction.BETWEEN_DEVICE_AND_CLOUD, ConflictException.class, FullShadowSyncRequest.class),
                arguments(Direction.BETWEEN_DEVICE_AND_CLOUD, ConflictError.class, FullShadowSyncRequest.class),
                arguments(Direction.BETWEEN_DEVICE_AND_CLOUD, UnknownShadowException.class, FullShadowSyncRequest.class)
        );
    }

}
