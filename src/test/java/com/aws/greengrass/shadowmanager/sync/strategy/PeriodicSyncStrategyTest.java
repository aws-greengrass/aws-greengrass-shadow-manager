/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync.strategy;

import com.aws.greengrass.shadowmanager.exception.RetryableException;
import com.aws.greengrass.shadowmanager.sync.RequestQueue;
import com.aws.greengrass.shadowmanager.sync.RequestMerger;
import com.aws.greengrass.shadowmanager.sync.model.CloudUpdateSyncRequest;
import com.aws.greengrass.shadowmanager.sync.model.Direction;
import com.aws.greengrass.shadowmanager.sync.model.DirectionWrapper;
import com.aws.greengrass.shadowmanager.sync.model.FullShadowSyncRequest;
import com.aws.greengrass.shadowmanager.sync.model.SyncRequest;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.atMostOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith({MockitoExtension.class, GGExtension.class})
class PeriodicSyncStrategyTest extends SyncStrategyTestBase<PeriodicSyncStrategy, ScheduledExecutorService> {

    DirectionWrapper direction = new DirectionWrapper();

    PeriodicSyncStrategyTest() {
        super(Executors::newSingleThreadScheduledExecutor);
    }

    @Override
    PeriodicSyncStrategy defaultTestInstance() {
        return new PeriodicSyncStrategy(executorService, mockRetryer, 3, mockRequestQueue, direction);
    }

    @Test
    void GIVEN_sync_request_WHEN_putSyncRequest_and_sync_loop_runs_THEN_request_is_executed_successfully()
            throws Exception {
        strategy = new PeriodicSyncStrategy(executorService, mockRetryer, 1,
                new RequestQueue(new RequestMerger(direction)), direction);
        strategy.start(mockSyncContext, 1);
        strategy.putSyncRequest(new FullShadowSyncRequest("thing", "shadow"));

        verify(mockRetryer, timeout(Duration.ofSeconds(7).toMillis()).times(1)).run(any(), any(), any());
    }


    @Test
    void GIVEN_sync_request_WHEN_putSyncRequest_and_syncing_is_stopped_THEN_request_is_not_added_to_queue()
            throws Exception {
        strategy.putSyncRequest(mock(FullShadowSyncRequest.class));

        verify(mockRequestQueue, timeout(Duration.ofSeconds(5).toMillis()).times(0)).put(any());
        verify(mockRetryer, timeout(Duration.ofSeconds(5).toMillis()).times(0)).run(any(), any(), any());
    }

    @Test
    void GIVEN_sync_request_WHEN_sync_stops_during_put_in_queue_THEN_request_is_removed_from_queue()
            throws Exception {
        doAnswer(i -> {
            strategy.syncing.set(false);
            return null;
        }).when(mockRequestQueue).put(any());

        strategy.start(mockSyncContext, 2);
        strategy.putSyncRequest(mockFullShadowSyncRequest);

        verify(mockRequestQueue, timeout(Duration.ofSeconds(5).toMillis()).times(1)).put(any());
        verify(mockRequestQueue, times(1)).remove(any());
        verify(mockRetryer, timeout(Duration.ofSeconds(5).toMillis()).times(0)).run(any(), any(), any());
    }

    @Test
    void GIVEN_sync_request_WHEN_queue_throws_interrupted_exception_THEN_sync_request_is_not_added(ExtensionContext extensionContext)
            throws Exception {
        ignoreExceptionOfType(extensionContext, InterruptedException.class);
        doThrow(InterruptedException.class).when(mockRequestQueue).put(any());

        strategy.start(mockSyncContext, 1);
        strategy.putSyncRequest(mockFullShadowSyncRequest);

        verify(mockRetryer, timeout(Duration.ofSeconds(5).toMillis()).times(0)).run(any(), any(), any());
        // check that we are interrupted by our "fake" exception. This also clears the thread state so cleanup
        // happens correctly
        assertThat(Thread.interrupted(), is(true));

        verify(mockRequestQueue, atMostOnce()).offer(any());
    }

    @Test
    void GIVEN_request_queue_WHEN_put_and_clear_THEN_queue_has_correct_number_of_requests() throws InterruptedException {
        strategy = new PeriodicSyncStrategy(executorService, mockRetryer, 3,
                new RequestQueue(new RequestMerger(direction)), direction);
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
            FullShadowSyncRequest request = requests.poll();
            return request == null ? mock(FullShadowSyncRequest.class) : request;
        }).when(mockRequestQueue).poll();
        when(mockRequestQueue.offerAndTake(request1, false)).thenReturn(request1);

        strategy.start(mockSyncContext, 1);
        strategy.putSyncRequest(new FullShadowSyncRequest("foo", "bar"));

        assertThat("executed request", executeLatch.await(5, TimeUnit.SECONDS), is(true));
        assertThat("take all requests", takeLatch.await(5, TimeUnit.SECONDS), is(true));
        verify(mockRequestQueue, times(1)).offerAndTake(request1, false);
    }

    @Test
    void GIVEN_sync_request_WHEN_sync_request_run_throws_Random_Exception_THEN_does_not_add_request_back(ExtensionContext extensionContext)
            throws Exception {
        ignoreExceptionOfType(extensionContext, RuntimeException.class);
        CountDownLatch executeLatch = new CountDownLatch(2); // 1 retry to fail, then 1 to succeed

        FullShadowSyncRequest request1 = mock(FullShadowSyncRequest.class);
        lenient().when(request1.getThingName()).thenReturn("thing1");
        lenient().when(request1.getShadowName()).thenReturn("shadow1");

        doAnswer(invocation -> {
            executeLatch.countDown();
            if (executeLatch.getCount() != 0) {
                throw new RuntimeException("foo");
            }
            return null;
        }).when(mockRetryer).run(any(), any(), any());

        when(mockRequestQueue.poll()).thenReturn(request1);

        strategy.start(mockSyncContext, 1);
        strategy.putSyncRequest(new FullShadowSyncRequest("foo", "bar"));

        assertThat("executed request", executeLatch.await(5, TimeUnit.SECONDS), is(true));
        verify(mockRequestQueue, atLeastOnce()).poll();
        verify(mockRequestQueue, times(0)).offerAndTake(request1, false);
    }

    @Test
    void GIVEN_no_sync_executing_WHEN_stop_THEN_no_wait(ExtensionContext extensionContext) throws InterruptedException {
        // set large interval so we can stop while the thread is scheduled but not executing
        strategy = new PeriodicSyncStrategy(executorService, mockRetryer, 300, mockRequestQueue, direction);
        strategy.start(mockSyncContext, 1);
        CountDownLatch done = new CountDownLatch(1);
        Executors.newSingleThreadExecutor().submit(() -> {
            while (strategy.isExecuting()) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    fail("interrupted");
                }
            }
            strategy.syncThreadLock.lock();
            try {
                strategy.doStop();
                done.countDown();
            } finally {
                strategy.syncThreadLock.unlock();
            }
        });

        assertThat("no wait for shutdown", done.await(1, TimeUnit.SECONDS), is(true));
    }

    @ParameterizedTest
    @MethodSource("expectedSyncRequestsOnConflictByDirection")
    void GIVEN_syncing_WHEN_error_THEN_full_sync(Direction direction, Class<? extends Throwable> error, Class<? extends SyncRequest> expectedSyncRequest, ExtensionContext extensionContext)
            throws Exception {
        ignoreExceptionOfType(extensionContext, error);

        this.direction.setDirection(direction);

        ExecutorService executorService = Executors.newSingleThreadExecutor();

        CloudUpdateSyncRequest request1 = mock(CloudUpdateSyncRequest.class);
        lenient().when(request1.getThingName()).thenReturn("thing1");
        lenient().when(request1.getShadowName()).thenReturn("shadow1");

        CountDownLatch executeLatch = new CountDownLatch(2); // 1 retry to fail, 1 to succeed

        when(mockRequestQueue.poll()).thenAnswer(invocation -> {
            if (executeLatch.getCount() == 2) {
                return request1;
            }
            // sleep for the subsequent take until thread shuts down
            TimeUnit.SECONDS.sleep(Duration.of(5, ChronoUnit.SECONDS).toMillis());
            return null;
        });

        doAnswer(invocation -> {
            executeLatch.countDown();
            throw (Throwable)mock(error);
        }).when(mockRetryer).run(any(), eq(request1), any());

        doAnswer(invocation -> {
            executeLatch.countDown();
            return null;
        }).when(mockRetryer).run(any(), any(expectedSyncRequest), any());

        // return the offered request
        when(mockRequestQueue.offerAndTake(any(expectedSyncRequest), eq(true)))
                .thenAnswer(invocation -> invocation.getArgument(0, expectedSyncRequest));

        try {
            strategy.start(mockSyncContext, 1);

            assertThat("executed requests", executeLatch.await(5, TimeUnit.SECONDS), is(true));
        } finally {
            executorService.shutdownNow();
        }

        ArgumentCaptor<SyncRequest> requestCaptor = ArgumentCaptor.forClass(SyncRequest.class);
        verify(mockRetryer, times(2)).run(any(), requestCaptor.capture(), any());
        assertThat(requestCaptor.getAllValues().get(0), is(request1));
        assertThat(requestCaptor.getAllValues().get(1), instanceOf(expectedSyncRequest));
        assertThat(requestCaptor.getAllValues().get(1).getThingName(), is("thing1"));
        assertThat(requestCaptor.getAllValues().get(1).getShadowName(), is("shadow1"));
    }
}
