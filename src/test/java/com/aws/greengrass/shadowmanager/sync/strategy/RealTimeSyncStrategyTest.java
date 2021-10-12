/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync.strategy;

import com.aws.greengrass.logging.impl.config.LogConfig;
import com.aws.greengrass.shadowmanager.exception.RetryableException;
import com.aws.greengrass.shadowmanager.exception.UnknownShadowException;
import com.aws.greengrass.shadowmanager.sync.RequestBlockingQueue;
import com.aws.greengrass.shadowmanager.sync.Retryer;
import com.aws.greengrass.shadowmanager.sync.model.CloudUpdateSyncRequest;
import com.aws.greengrass.shadowmanager.sync.model.FullShadowSyncRequest;
import com.aws.greengrass.shadowmanager.sync.model.SyncContext;
import com.aws.greengrass.shadowmanager.sync.model.SyncRequest;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.event.Level;
import software.amazon.awssdk.aws.greengrass.model.ConflictError;
import software.amazon.awssdk.services.iotdataplane.model.ConflictException;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.hamcrest.core.Is.is;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith({MockitoExtension.class, GGExtension.class})
class RealTimeSyncStrategyTest {

    @Mock
    private Retryer mockRetryer;
    @Mock
    private SyncContext mockSyncContext;
    @Mock
    private RequestBlockingQueue mockRequestBlockingQueue;

    private ExecutorService executorService;
    private RealTimeSyncStrategy strategy;

    @BeforeEach
    void setup() {
        LogConfig.getRootLogConfig().setLevel(Level.ERROR);
        executorService = Executors.newCachedThreadPool();
    }

    @AfterEach
    void tearDown() {
        strategy.stop();
        executorService.shutdownNow();
    }

    @Test
    void GIVEN_sync_request_WHEN_putSyncRequest_and_sync_loop_runs_THEN_request_is_executed_successfully()
            throws Exception {
        strategy = new RealTimeSyncStrategy(executorService, mockRetryer);
        strategy.start(mockSyncContext, 1);
        strategy.putSyncRequest(mock(FullShadowSyncRequest.class));

        verify(mockRetryer, timeout(Duration.ofSeconds(5).toMillis()).times(1)).run(any(), any(), any());
    }

    @Test
    void GIVEN_sync_request_WHEN_putSyncRequest_and_syncing_is_stopped_THEN_request_is_not_added_to_queue()
            throws Exception {
        strategy = new RealTimeSyncStrategy(executorService, mockRetryer);
        strategy.setSyncQueue(mockRequestBlockingQueue);

        strategy.putSyncRequest(mock(FullShadowSyncRequest.class));

        verify(mockRequestBlockingQueue, timeout(Duration.ofSeconds(5).toMillis()).times(0)).put(any());
        verify(mockRetryer, timeout(Duration.ofSeconds(5).toMillis()).times(0)).run(any(), any(), any());
    }

    @Test
    void GIVEN_sync_request_WHEN_sync_stops_during_put_in_queue_THEN_request_is_removed_from_queue()
            throws Exception {
        strategy = new RealTimeSyncStrategy(executorService, mockRetryer);
        strategy.setSyncQueue(mockRequestBlockingQueue);

        doAnswer(i -> {
            strategy.syncing.set(false);
            return null;
        }).when(mockRequestBlockingQueue).put(any());

        strategy.start(mockSyncContext, 1);
        strategy.putSyncRequest(mock(FullShadowSyncRequest.class));

        verify(mockRequestBlockingQueue, timeout(Duration.ofSeconds(5).toMillis()).times(1)).put(any());
        verify(mockRequestBlockingQueue, times(1)).remove(any());
        verify(mockRetryer, timeout(Duration.ofSeconds(5).toMillis()).times(0)).run(any(), any(), any());
    }

    @Test
    void GIVEN_sync_request_WHEN_queue_throws_interrupted_exception_THEN_sync_request_is_not_added(ExtensionContext extensionContext)
            throws Exception {
        ignoreExceptionOfType(extensionContext, InterruptedException.class);
        strategy = new RealTimeSyncStrategy(executorService, mockRetryer);
        strategy.setSyncQueue(mockRequestBlockingQueue);
        doThrow(InterruptedException.class).when(mockRequestBlockingQueue).put(any());

        strategy.start(mockSyncContext, 1);
        strategy.putSyncRequest(mock(FullShadowSyncRequest.class));

        verify(mockRetryer, timeout(Duration.ofSeconds(5).toMillis()).times(0)).run(any(), any(), any());
        // check that we are interrupted by our "fake" exception. This also clears the thread state so cleanup
        // happens correctly
        assertThat(Thread.interrupted(), is(true));

    }

    @Test
    void GIVEN_request_queue_WHEN_put_and_clear_THEN_queue_has_correct_number_of_requests() throws InterruptedException {
        strategy = new RealTimeSyncStrategy(executorService, mockRetryer);

        strategy.syncing.set(true);

        Random rand = new Random();
        int randomNumberOfSyncRequests = rand.nextInt(1024);
        for (int i = 0; i < randomNumberOfSyncRequests; i++) {
            strategy.putSyncRequest(new FullShadowSyncRequest("foo-" + i, "bar-" + i));
        }
        assertThat(strategy.getRemainingCapacity(), is(1024 - randomNumberOfSyncRequests));

        strategy.clearSyncQueue();

        assertThat(strategy.getRemainingCapacity(), is(1024));
    }

    @Test
    void GIVEN_sync_request_WHEN_sync_request_run_throws_RetryableException_THEN_adds_the_sync_request_back(ExtensionContext extensionContext)
            throws Exception {
        ignoreExceptionOfType(extensionContext, RetryableException.class);
        CountDownLatch executeLatch = new CountDownLatch(2); // 1 retry to fail, then 1 to succeed

        FullShadowSyncRequest request1 = mock(FullShadowSyncRequest.class);
        lenient().when(request1.getThingName()).thenReturn("thing1");
        lenient().when(request1.getShadowName()).thenReturn("shadow1");

        FullShadowSyncRequest request2 = mock(FullShadowSyncRequest.class);
        lenient().when(request2.getThingName()).thenReturn("thing2");
        lenient().when(request2.getShadowName()).thenReturn("shadow2");

        Queue<FullShadowSyncRequest> requests = new LinkedList<>();
        requests.add(request1);
        requests.add(request2);

        strategy = new RealTimeSyncStrategy(executorService, mockRetryer);
        strategy.setSyncQueue(mockRequestBlockingQueue);
        doAnswer(invocation -> {
            executeLatch.countDown();
            if (executeLatch.getCount() != 0) {
                throw new RetryableException(new RuntimeException("foo"));
            }
            return null;
        }).when(mockRetryer).run(any(), any(), any());

        CountDownLatch takeLatch = new CountDownLatch(2);
        doAnswer(invocation -> {
            takeLatch.countDown();
            return requests.poll();
        }).when(mockRequestBlockingQueue).take();
        when(mockRequestBlockingQueue.offerAndTake(request1, false)).thenReturn(request1);

        strategy.start(mockSyncContext, 1);
        strategy.putSyncRequest(new FullShadowSyncRequest("foo", "bar"));

        assertThat("executed request", executeLatch.await(5, TimeUnit.SECONDS), is(true));
        assertThat("take all requests", takeLatch.await(5, TimeUnit.SECONDS), is(true));
        verify(mockRequestBlockingQueue, times(1)).offerAndTake(request1, false);
    }

    @Test
    void GIVEN_sync_request_WHEN_sync_request_run_throws_Random_Exception_THEN_does_not_add_request_back(ExtensionContext extensionContext)
            throws Exception {
        ignoreExceptionOfType(extensionContext, RuntimeException.class);
        CountDownLatch executeLatch = new CountDownLatch(2); // 1 retry to fail, then 1 to succeed

        FullShadowSyncRequest request1 = mock(FullShadowSyncRequest.class);
        lenient().when(request1.getThingName()).thenReturn("thing1");
        lenient().when(request1.getShadowName()).thenReturn("shadow1");

        strategy = new RealTimeSyncStrategy(executorService, mockRetryer);
        strategy.setSyncQueue(mockRequestBlockingQueue);
        doAnswer(invocation -> {
            executeLatch.countDown();
            if (executeLatch.getCount() != 0) {
                throw new RuntimeException("foo");
            }
            return null;
        }).when(mockRetryer).run(any(), any(), any());

        when(mockRequestBlockingQueue.take()).thenReturn(request1);

        strategy.start(mockSyncContext, 1);
        strategy.putSyncRequest(new FullShadowSyncRequest("foo", "bar"));

        assertThat("executed request", executeLatch.await(5, TimeUnit.SECONDS), is(true));
        verify(mockRequestBlockingQueue, atLeastOnce()).take();
        verify(mockRequestBlockingQueue, times(0)).offerAndTake(request1, false);
    }

    @ParameterizedTest
    @ValueSource(classes = { ConflictException.class, ConflictError.class, UnknownShadowException.class})
    void GIVEN_syncing_WHEN_error_THEN_full_sync(Class clazz, ExtensionContext extensionContext)
            throws Exception {
        ignoreExceptionOfType(extensionContext, clazz);

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        strategy = new RealTimeSyncStrategy(executorService, mockRetryer);
        strategy.setSyncQueue(mockRequestBlockingQueue);

        CloudUpdateSyncRequest request1 = mock(CloudUpdateSyncRequest.class);
        lenient().when(request1.getThingName()).thenReturn("thing1");
        lenient().when(request1.getShadowName()).thenReturn("shadow1");

        CountDownLatch executeLatch = new CountDownLatch(2); // 1 retry to fail, 1 to succeed

        when(mockRequestBlockingQueue.take()).thenAnswer(invocation -> {
            if (executeLatch.getCount() == 2) {
                return request1;
            }
            // sleep for the subsequent take until thread shuts down
            Thread.sleep(Duration.of(5, ChronoUnit.SECONDS).toMillis());
            return null;
        });

        doAnswer(invocation -> {
            executeLatch.countDown();
            throw (Throwable)mock(clazz);
        }).when(mockRetryer).run(any(), eq(request1), any());

        doAnswer(invocation -> {
            executeLatch.countDown();
            return null;
        }).when(mockRetryer).run(any(), any(FullShadowSyncRequest.class), any());

        // return the offered request
        when(mockRequestBlockingQueue.offerAndTake(any(FullShadowSyncRequest.class), eq(true)))
                .thenAnswer(invocation -> invocation.getArgument(0, FullShadowSyncRequest.class));

        try {
            strategy.start(mockSyncContext, 1);

            assertThat("executed requests", executeLatch.await(5, TimeUnit.SECONDS), is(true));
        } finally {
            executorService.shutdownNow();
        }

        ArgumentCaptor<SyncRequest> requestCaptor = ArgumentCaptor.forClass(SyncRequest.class);
        verify(mockRetryer, times(2)).run(any(), requestCaptor.capture(), any());
        assertThat(requestCaptor.getAllValues().get(0), is(request1));
        assertThat(requestCaptor.getAllValues().get(1), instanceOf(FullShadowSyncRequest.class));
        assertThat(requestCaptor.getAllValues().get(1).getThingName(), is("thing1"));
        assertThat(requestCaptor.getAllValues().get(1).getShadowName(), is("shadow1"));
    }
}
