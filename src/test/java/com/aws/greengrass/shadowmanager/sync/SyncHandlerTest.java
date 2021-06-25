/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync;

import com.aws.greengrass.shadowmanager.exception.RetryableException;
import com.aws.greengrass.shadowmanager.exception.SkipSyncRequestException;
import com.aws.greengrass.shadowmanager.exception.UnknownShadowException;
import com.aws.greengrass.shadowmanager.model.configuration.ThingShadowSyncConfiguration;
import com.aws.greengrass.shadowmanager.sync.model.CloudDeleteSyncRequest;
import com.aws.greengrass.shadowmanager.sync.model.CloudUpdateSyncRequest;
import com.aws.greengrass.shadowmanager.sync.model.FullShadowSyncRequest;
import com.aws.greengrass.shadowmanager.sync.model.LocalDeleteSyncRequest;
import com.aws.greengrass.shadowmanager.sync.model.LocalUpdateSyncRequest;
import com.aws.greengrass.shadowmanager.sync.model.SyncContext;
import com.aws.greengrass.shadowmanager.sync.model.SyncRequest;
import com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.util.Pair;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.aws.greengrass.model.ConflictError;
import software.amazon.awssdk.services.iotdataplane.model.ConflictException;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith({MockitoExtension.class, GGExtension.class})
class SyncHandlerTest {

    @Mock
    RequestBlockingQueue queue;

    @Mock
    ExecutorService executorService;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    SyncContext context;

    SyncHandler syncHandler;

    @BeforeEach
    void setup() {
        lenient().when(queue.remainingCapacity()).thenReturn(1024);
        syncHandler = new SyncHandler(queue, executorService);
    }

    @Test
    void GIVEN_not_started_WHEN_start_THEN_full_sync() throws InterruptedException {
        // GIVEN
        int numThreads = 3;

        when(executorService.submit(any(Runnable.class))).thenReturn(mock(Future.class));

        List<Pair<String, String>> shadows = Arrays.asList(new Pair<>("a", "1"), new Pair<>("b", "2"));
        when(context.getDao().listSyncedShadows()).thenReturn(shadows);

        // WHEN
        syncHandler.start(context, numThreads);

        // THEN
        verify(queue, times(1)).clear();
        verify(queue, times(shadows.size())).put(any());
        assertThat(syncHandler.syncThreads, hasSize(numThreads));
    }

    @Test
    void GIVEN_started_WHEN_start_again_THEN_do_nothing() throws InterruptedException {
        // GIVEN
        int numThreads = 3;
        when(executorService.submit(any(Runnable.class))).thenReturn(mock(Future.class));

        List<Pair<String, String>> shadows = Arrays.asList(new Pair<>("a", "1"),
                new Pair<>("b", "2"));
        when(context.getDao().listSyncedShadows()).thenReturn(shadows);

        syncHandler.start(context, numThreads);

        // WHEN - start again
        syncHandler.start(context, numThreads);

        // THEN - did nothing different from starting
        verify(queue, times(1)).clear();
        verify(queue, times(shadows.size())).put(any());
        assertThat(syncHandler.syncThreads, hasSize(numThreads));
    }
    
    @Test
    void GIVEN_not_started_WHEN_stop_THEN_do_nothing() {
        // WHEN
        syncHandler.stop();

        // THEN
        verify(queue, never()).clear();
    }

    @Test
    void GIVEN_started_WHEN_stop_THEN_stop_threads() {
        // GIVEN
        int numThreads = 1;

        Future syncThread = mock(Future.class);
        when(executorService.submit(any(Runnable.class))).thenReturn(syncThread);

        List<Pair<String, String>> shadows = Arrays.asList(new Pair<>("a", "1"), new Pair<>("b", "2"));
        when(context.getDao().listSyncedShadows()).thenReturn(shadows);

        syncHandler.start(context, numThreads);
        assertThat(syncHandler.syncThreads, hasSize(numThreads));

        // WHEN
        syncHandler.stop();

        // THEN
        verify(syncThread, times(1)).cancel(true);
        verify(queue, times(2)).clear(); // full sync and stop both clear
        assertThat(syncHandler.syncThreads, hasSize(0));
    }

    @Test
    void GIVEN_not_started_WHEN_put_sync_request_THEN_request_not_added() throws InterruptedException {
        // WHEN
        FullShadowSyncRequest request = new FullShadowSyncRequest("foo", "bar");
        syncHandler.putSyncRequest(request);

        // THEN
        verify(queue, never()).put(request);
    }

    @Test
    void GIVEN_started_WHEN_full_shadow_sync_interrupted_THEN_stop() throws InterruptedException {
        List<Pair<String, String>> shadows = Arrays.asList(new Pair<>("a", "1"), new Pair<>("b", "2"));
        when(context.getDao().listSyncedShadows()).thenReturn(shadows);

        doThrow(new InterruptedException()).when(queue).put(any());

        Future syncThread = mock(Future.class);
        when(executorService.submit(any(Runnable.class))).thenReturn(syncThread);

        // GIVEN
        syncHandler.start(context, 1);

        verify(syncThread, times(1)).cancel(true);
        assertThat("syncing", syncHandler.syncing.get(), is(false));

        // check that we are interrupted by our "fake" exception. This also clears the thread state so cleanup
        // happens correctly
        assertThat(Thread.interrupted(), is(true));
    }

    @Test
    void GIVEN_started_and_sync_request_added_WHEN_syncing_stopped_THEN_remove_sync_request() throws InterruptedException {
        // GIVEN
        syncHandler.syncing.set(true);

        FullShadowSyncRequest request = new FullShadowSyncRequest("foo", "bar");

        // WHEN
        doAnswer(invocation -> {
            syncHandler.syncing.set(false);
            return null;
        }).when(queue).put(request);

        syncHandler.putSyncRequest(request);

        // THEN
        verify(queue, times(1)).remove(request);
    }

    @Test
    void GIVEN_request_added_to_queue_WHEN_request_taken_from_queue_THEN_execute_request() throws Exception {
        RequestBlockingQueue queue = new RequestBlockingQueue(new RequestMerger());
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        syncHandler = new SyncHandler(queue, executorService);

        when(context.getDao().listSyncedShadows()).thenReturn(Collections.emptyList());

        FullShadowSyncRequest request = mock(FullShadowSyncRequest.class);
        CountDownLatch executeLatch = new CountDownLatch(1);
        doAnswer(invocation -> {
            executeLatch.countDown();
            return null;
        }).when(request).execute(context);
        try {
            syncHandler.start(context, 1);

            lenient().when(request.getThingName()).thenReturn("thing");
            lenient().when(request.getShadowName()).thenReturn("shadow");

            assertThat("can add item to queue", queue.offer(request), is(true));
            assertThat("executed request", executeLatch.await(5, TimeUnit.SECONDS), is(true));
        } finally {
            executorService.shutdownNow();
        }
        verify(request, times(1)).execute(context);
    }

    @Test
    void GIVEN_syncing_WHEN_item_execute_fails_with_retry_THEN_retry(ExtensionContext extensionContext) throws
            Exception {
        ExceptionLogProtector.ignoreExceptionOfType(extensionContext, RetryableException.class);
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        SyncHandler.Retryer retryer = mock(SyncHandler.Retryer.class);
        syncHandler = new SyncHandler(queue, executorService, retryer);

        when(context.getDao().listSyncedShadows()).thenReturn(Collections.emptyList());


        FullShadowSyncRequest request1 = mock(FullShadowSyncRequest.class);
        lenient().when(request1.getThingName()).thenReturn("thing1");
        lenient().when(request1.getShadowName()).thenReturn("shadow1");

        FullShadowSyncRequest request2 = mock(FullShadowSyncRequest.class);
        lenient().when(request2.getThingName()).thenReturn("thing2");
        lenient().when(request2.getShadowName()).thenReturn("shadow2");

        CountDownLatch executeLatch = new CountDownLatch(2); // 1 retry to fail, then 1 to succeed

        Queue<FullShadowSyncRequest> requests = new LinkedList<>();
        requests.add(request1);
        requests.add(request2);

        CountDownLatch takeLatch = new CountDownLatch(2);
        doAnswer(invocation -> {
            takeLatch.countDown();
            return requests.poll();
        }).when(queue).take();

        doAnswer(invocation -> {
            executeLatch.countDown();
            if (executeLatch.getCount() != 0) {
                throw new RetryableException(new RuntimeException("foo"));
            }
            return null;
        }).when(retryer).run(any(), eq(request1), eq(context));

        when(queue.offerAndTake(request1, false)).thenReturn(request1);

        try {
            syncHandler.start(context, 1);

            assertThat("executed request", executeLatch.await(5, TimeUnit.SECONDS), is(true));
            assertThat("take all requests", takeLatch.await(5, TimeUnit.SECONDS), is(true));
        } finally {
            executorService.shutdownNow();
        }

        verify(queue, times(1)).offerAndTake(request1, false);
    }

    @Test
    void GIVEN_syncing_WHEN_item_execute_fails_with_skip_THEN_skip(ExtensionContext extensionContext) throws Exception {
        ExceptionLogProtector.ignoreExceptionOfType(extensionContext, SkipSyncRequestException.class);
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        SyncHandler.Retryer retryer = mock(SyncHandler.Retryer.class);
        syncHandler = new SyncHandler(queue, executorService, retryer);

        when(context.getDao().listSyncedShadows()).thenReturn(Collections.emptyList());

        FullShadowSyncRequest request1 = mock(FullShadowSyncRequest.class);
        lenient().when(request1.getThingName()).thenReturn("thing1");
        lenient().when(request1.getShadowName()).thenReturn("shadow1");

        FullShadowSyncRequest request2 = mock(FullShadowSyncRequest.class);
        lenient().when(request2.getThingName()).thenReturn("thing2");
        lenient().when(request2.getShadowName()).thenReturn("shadow2");

        CountDownLatch executeLatch = new CountDownLatch(2); // 1 retry to fail, then 1 to succeed

        when(queue.take()).thenReturn(request1, request2);

        doAnswer(invocation -> {
            executeLatch.countDown();
            throw new SkipSyncRequestException(new RuntimeException("foo"));
        }).when(retryer).run(any(), eq(request1), eq(context));

        doAnswer(invocation -> {
            executeLatch.countDown();
            // sleep here so we can ensure the latch finishes without multiple retries of request2
            try {
                TimeUnit.SECONDS.sleep(5);
            } catch (InterruptedException e) {
                // ignore this - we expect to be interupted when stopping
            }
            return null;
        }).when(retryer).run(any(), eq(request2), eq(context));

        try {
            syncHandler.start(context, 1);

            assertThat("executed requests", executeLatch.await(5, TimeUnit.SECONDS), is(true));
        } finally {
            executorService.shutdownNow();
        }
    }

    @Test
    void GIVEN_syncing_WHEN_item_execute_interrupted_THEN_stop_taking_items(ExtensionContext extensionContext)
            throws Exception {
        ExceptionLogProtector.ignoreExceptionOfType(extensionContext, SkipSyncRequestException.class);
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        SyncHandler.Retryer retryer = mock(SyncHandler.Retryer.class);
        syncHandler = new SyncHandler(queue, executorService, retryer);

        when(context.getDao().listSyncedShadows()).thenReturn(Collections.emptyList());

        FullShadowSyncRequest request1 = mock(FullShadowSyncRequest.class);
        lenient().when(request1.getThingName()).thenReturn("thing1");
        lenient().when(request1.getShadowName()).thenReturn("shadow1");

        CountDownLatch executeLatch = new CountDownLatch(1); // 1 retry to fail

        when(queue.take()).thenReturn(request1);

        doAnswer(invocation -> {
            executeLatch.countDown();
            throw new InterruptedException();
        }).when(retryer).run(any(), eq(request1), eq(context));


        try {
            syncHandler.start(context, 1);

            assertThat("executed requests", executeLatch.await(5, TimeUnit.SECONDS), is(true));
        } finally {
            executorService.shutdownNow();
        }

        verify(queue, times(1)).take();
    }

    @ParameterizedTest
    @ValueSource(classes = { ConflictException.class, ConflictError.class, UnknownShadowException.class})
    void GIVEN_syncing_WHEN_error_THEN_full_sync(Class clazz, ExtensionContext extensionContext)
            throws Exception {
        ExceptionLogProtector.ignoreExceptionOfType(extensionContext, clazz);

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        SyncHandler.Retryer retryer = mock(SyncHandler.Retryer.class);
        syncHandler = new SyncHandler(queue, executorService, retryer);

        when(context.getDao().listSyncedShadows()).thenReturn(Collections.emptyList());

        CloudUpdateSyncRequest request1 = mock(CloudUpdateSyncRequest.class);
        lenient().when(request1.getThingName()).thenReturn("thing1");
        lenient().when(request1.getShadowName()).thenReturn("shadow1");

        CountDownLatch executeLatch = new CountDownLatch(2); // 1 retry to fail, 1 to succeed

        when(queue.take()).thenAnswer(invocation -> {
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
        }).when(retryer).run(any(), eq(request1), eq(context));

        doAnswer(invocation -> {
            executeLatch.countDown();
            return null;
        }).when(retryer).run(any(), any(FullShadowSyncRequest.class), eq(context));

        // return the offered request
        when(queue.offerAndTake(any(FullShadowSyncRequest.class), eq(true)))
                .thenAnswer(invocation -> invocation.getArgument(0, FullShadowSyncRequest.class));

        try {
            syncHandler.start(context, 1);

            assertThat("executed requests", executeLatch.await(5, TimeUnit.SECONDS), is(true));
        } finally {
            executorService.shutdownNow();
        }

        ArgumentCaptor<SyncRequest> requestCaptor = ArgumentCaptor.forClass(SyncRequest.class);
        verify(retryer, times(2)).run(any(), requestCaptor.capture(), any());
        assertThat(requestCaptor.getAllValues().get(0), is(request1));
        assertThat(requestCaptor.getAllValues().get(1), instanceOf(FullShadowSyncRequest.class));
        assertThat(requestCaptor.getAllValues().get(1).getThingName(), is("thing1"));
        assertThat(requestCaptor.getAllValues().get(1).getShadowName(), is("shadow1"));
    }

    @Test
    void GIVEN_syncing_WHEN_push_requests_THEN_requests_added() throws InterruptedException {
        Set<ThingShadowSyncConfiguration> syncConfigurations = new HashSet<>();
        syncConfigurations.add(ThingShadowSyncConfiguration.builder().thingName("thing1").shadowName("shadow1").build());
        syncConfigurations.add(ThingShadowSyncConfiguration.builder().thingName("thing2").shadowName("shadow2").build());
        syncConfigurations.add(ThingShadowSyncConfiguration.builder().thingName("thing3").shadowName("shadow3").build());
        syncConfigurations.add(ThingShadowSyncConfiguration.builder().thingName("thing4").shadowName("shadow4").build());
        syncConfigurations.add(ThingShadowSyncConfiguration.builder().thingName("thing5").shadowName("shadow5").build());
        syncHandler.setSyncConfiguration(syncConfigurations);
        syncHandler.syncing.set(true);

        JsonNode node = mock(JsonNode.class);

        ArgumentCaptor<SyncRequest> requestCaptor = ArgumentCaptor.forClass(SyncRequest.class);

        syncHandler.pushLocalUpdateSyncRequest("thing1", "shadow1", "".getBytes(StandardCharsets.UTF_8));
        syncHandler.pushCloudUpdateSyncRequest("thing2", "shadow2", node);
        syncHandler.pushLocalDeleteSyncRequest("thing3", "shadow3", "".getBytes(StandardCharsets.UTF_8));
        syncHandler.pushCloudDeleteSyncRequest("thing4", "shadow4");
        syncHandler.fullSyncOnShadow("thing5", "shadow5");

        verify(queue, times(5)).put(requestCaptor.capture());

        assertThat(requestCaptor.getAllValues().get(0), isA(LocalUpdateSyncRequest.class));
        assertThat(requestCaptor.getAllValues().get(1), isA(CloudUpdateSyncRequest.class));
        assertThat(requestCaptor.getAllValues().get(2), isA(LocalDeleteSyncRequest.class));
        assertThat(requestCaptor.getAllValues().get(3), isA(CloudDeleteSyncRequest.class));
        assertThat(requestCaptor.getAllValues().get(4), isA(FullShadowSyncRequest.class));

        assertThat(requestCaptor.getAllValues().get(0).getThingName(), is("thing1"));
        assertThat(requestCaptor.getAllValues().get(1).getThingName(), is("thing2"));
        assertThat(requestCaptor.getAllValues().get(2).getThingName(), is("thing3"));
        assertThat(requestCaptor.getAllValues().get(3).getThingName(), is("thing4"));
        assertThat(requestCaptor.getAllValues().get(4).getThingName(), is("thing5"));

        assertThat(requestCaptor.getAllValues().get(0).getShadowName(), is("shadow1"));
        assertThat(requestCaptor.getAllValues().get(1).getShadowName(), is("shadow2"));
        assertThat(requestCaptor.getAllValues().get(2).getShadowName(), is("shadow3"));
        assertThat(requestCaptor.getAllValues().get(3).getShadowName(), is("shadow4"));
        assertThat(requestCaptor.getAllValues().get(4).getShadowName(), is("shadow5"));
    }


}
