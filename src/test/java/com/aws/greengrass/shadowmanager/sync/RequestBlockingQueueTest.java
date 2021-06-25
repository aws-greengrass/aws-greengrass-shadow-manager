/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync;

import com.aws.greengrass.shadowmanager.sync.model.SyncRequest;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith({MockitoExtension.class, GGExtension.class})
public class RequestBlockingQueueTest {

    private static final long WAIT_SECONDS = 5;

    RequestBlockingQueue queue;

    @Mock
    RequestMerger merger;

    @Mock
    SyncRequest thingAShadow1;
    @Mock
    SyncRequest thingAShadow2;
    @Mock
    SyncRequest thingBShadow1;
    @Mock
    SyncRequest thingCShadow1;
    @Mock
    SyncRequest thingAShadow1Again;
    @Mock
    SyncRequest thingAShadow1Merged;

    @BeforeEach
    void setup() {
        queue = new RequestBlockingQueue(merger, 3);
        setupRequest(thingAShadow1, "A", "1");
        setupRequest(thingAShadow1Again, "A", "1");
        setupRequest(thingAShadow2, "A", "2");
        setupRequest(thingBShadow1, "B", "1");
        setupRequest(thingCShadow1, "C", "1");
    }

    void setupRequest(SyncRequest req, String thing, String shadow) {
        lenient().when(req.getThingName()).thenReturn(thing);
        lenient().when(req.getShadowName()).thenReturn(shadow);
    }

    void waitLatch(CountDownLatch latch) {
        boolean finished = assertDoesNotThrow(() -> latch.await(WAIT_SECONDS, TimeUnit.SECONDS),
                "waiting on latch is interrupted");
        assertThat("timed out waiting on latch", finished, is(true));
    }

    @Test
    void GIVEN_empty_queue_THEN_queue_is_empty() {
        assertThat(queue.size(), is(0));
        assertThat("isEmpty", queue.isEmpty(), is(true));
        assertThat("isFull", queue.isFull(), is(false));
    }

    @Test
    void GIVEN_empty_queue_WHEN_add_items_THEN_queue_fills() {
        assertThat("isEmpty", queue.isEmpty(), is(true));
        assertThat("isFull", queue.isFull(), is(false));
        assertThat("capacity", queue.remainingCapacity(), is(3));

        assertThat(queue.offer(thingAShadow1), is(true));
        assertThat(queue.size(), is(1));
        assertThat("isEmpty", queue.isEmpty(), is(false));
        assertThat("isFull", queue.isFull(), is(false));
        assertThat("capacity", queue.remainingCapacity(), is(2));

        assertThat(queue.offer(thingAShadow2), is(true));
        assertThat(queue.size(), is(2));
        assertThat("isEmpty", queue.isEmpty(), is(false));
        assertThat("isFull", queue.isFull(), is(false));
        assertThat("capacity", queue.remainingCapacity(), is(1));

        assertThat(queue.offer(thingBShadow1), is(true));
        assertThat(queue.size(), is(3));
        assertThat("isEmpty", queue.isEmpty(), is(false));
        assertThat("isFull", queue.isFull(), is(true));
        assertThat("capacity", queue.remainingCapacity(), is(0));
    }

    @Test
    void GIVEN_empty_queue_WHEN_poll_THEN_returns_null() throws InterruptedException {
        assertThat(queue.poll(), is(nullValue()));
        assertThat(queue.poll(WAIT_SECONDS, TimeUnit.SECONDS), is(nullValue()));
    }

    @Test
    void GIVEN_empty_queue_WHEN_peek_THEN_returns_null() {
        assertThat(queue.peek(), is(nullValue()));
    }

    @Test
    void GIVEN_non_empty_queue_WHEN_poll_THEN_returns_values_in_order() {
        queue.offer(thingAShadow1);
        queue.offer(thingAShadow2);
        queue.offer(thingBShadow1);
        assertThat(queue.poll(), is(thingAShadow1));
        assertThat(queue.poll(), is(thingAShadow2));
        assertThat(queue.poll(), is(thingBShadow1));
        assertThat(queue.poll(), is(nullValue()));
    }

    @Test
    void GIVEN_non_empty_queue_WHEN_poll_with_timeout_THEN_returns_values_in_order() throws InterruptedException {
        assertThat(queue.offer(thingAShadow1, WAIT_SECONDS, TimeUnit.SECONDS), is(true));
        assertThat(queue.offer(thingAShadow2, WAIT_SECONDS, TimeUnit.SECONDS), is(true));
        assertThat(queue.offer(thingBShadow1, WAIT_SECONDS, TimeUnit.SECONDS), is(true));
        assertThat(queue.poll(WAIT_SECONDS, TimeUnit.SECONDS), is(thingAShadow1));
        assertThat(queue.poll(WAIT_SECONDS, TimeUnit.SECONDS), is(thingAShadow2));
        assertThat(queue.poll(WAIT_SECONDS, TimeUnit.SECONDS), is(thingBShadow1));
        assertThat(queue.poll(WAIT_SECONDS, TimeUnit.SECONDS), is(nullValue()));
    }

    @Test
    void GIVEN_items_added_to_queue_WHEN_peek_THEN_does_not_remove_value() {
        assertThat(queue.peek(), is(nullValue()));
        queue.offer(thingAShadow1);
        assertThat(queue.peek(), is(thingAShadow1));
        queue.offer(thingAShadow2);
        assertThat(queue.peek(), is(thingAShadow1));

        queue.poll();

        assertThat(queue.peek(), is(thingAShadow2));
        queue.offer(thingBShadow1);
        assertThat(queue.peek(), is(thingAShadow2));
    }
    @Test
    void GIVEN_consumer_thread_taking_from_queue_WHEN_producer_thread_offers_item_THEN_consumer_receives_it() {
        AtomicReference<SyncRequest> received = new AtomicReference<>();
        CountDownLatch consumerLatch = new CountDownLatch(1);
        CountDownLatch producerLatch = new CountDownLatch(1);
        CountDownLatch consumerStartedLatch = new CountDownLatch(1);
        Thread consumer = new Thread(() -> {
            consumerStartedLatch.countDown();
            SyncRequest r = assertDoesNotThrow(() -> queue.take(), "waiting for queue.take interrupted");
            received.set(r);
            consumerLatch.countDown();
        });

        Thread producer = new Thread(() -> {
            // wait for consumer to start
            assertDoesNotThrow((Executable) consumerStartedLatch::await);
            queue.offer(thingAShadow1);
            producerLatch.countDown();
        });

        consumer.start();
        producer.start();

        waitLatch(producerLatch);
        waitLatch(consumerLatch);
        assertThat(received.get(), is(thingAShadow1));
    }

    @Test
    void GIVEN_full_queue_WHEN_add_THEN_item_not_added() {
        queue.offer(thingAShadow1);
        queue.offer(thingAShadow2);
        queue.offer(thingBShadow1);
        assertThat("queue full", queue.isFull(), is(true));

        assertThat("item added to full queue", queue.offer(thingCShadow1), is(false)) ;
        assertThat("item added to full queue with timeout",
                assertDoesNotThrow(() -> queue.offer(thingCShadow1, WAIT_SECONDS, TimeUnit.SECONDS)), is(false));
    }

    @Test
    void GIVEN_request_exists_in_queue_WHEN_add_request_for_same_shadow_THEN_item_merged() {
        queue.offer(thingAShadow1);
        assertThat(queue.size(), is(1));

        SyncRequest req = mock(SyncRequest.class);
        setupRequest(req, "A", "1");

        SyncRequest merged = mock(SyncRequest.class);
        when(merger.merge(any(), any())).thenReturn(merged);

        queue.offer(req);
        assertThat(queue.size(), is(1));


        SyncRequest actual = queue.poll();
        assertThat(actual, is(merged));
    }

    @Test
    void GIVEN_non_empty_queue_WHEN_clear_THEN_queue_empty() {
        queue.offer(thingAShadow1);
        queue.offer(thingAShadow2);
        assertThat("queue empty", queue.isEmpty(), is(false));

        queue.clear();
        assertThat("queue empty", queue.isEmpty(), is(true));
    }

    @Test
    void GIVEN_put_WHEN_not_full_THEN_returns_immediately() {
        CountDownLatch latch = new CountDownLatch(1);
        Thread runner = new Thread(() -> {
            assertDoesNotThrow(() -> queue.put(thingAShadow1));
            latch.countDown();
        });

        runner.start();

        waitLatch(latch);

        assertThat(queue.size(), is(1));
        assertThat(queue.poll(), is(thingAShadow1));
    }

    @Test
    void GIVEN_put_WHEN_full_THEN_waits_until_has_space() {
        assertThat("request added", queue.offer(thingAShadow1), is(true));
        assertThat("request added", queue.offer(thingAShadow2), is(true));
        assertThat("request added", queue.offer(thingBShadow1), is(true));
        assertThat("is full", queue.isFull(), is(true));

        CountDownLatch producerLatch = new CountDownLatch(1);
        Thread producer = new Thread(() -> {
            assertDoesNotThrow(() -> queue.put(thingCShadow1));
            producerLatch.countDown();
        });

        producer.start();

        CountDownLatch consumerLatch = new CountDownLatch(1);
        AtomicReference<SyncRequest> request = new AtomicReference<>();
        Thread consumer = new Thread(() -> {
            SyncRequest r = assertDoesNotThrow(() -> queue.take());
            request.set(r);
            consumerLatch.countDown();
        });

        consumer.start();

        waitLatch(producerLatch);
        waitLatch(consumerLatch);

        assertThat("is full", queue.isFull(), is(true));
        assertThat(request.get(), is(thingAShadow1));
        assertThat(queue.poll(), is(thingAShadow2));
        assertThat(queue.poll(), is(thingBShadow1));
        assertThat(queue.poll(), is(thingCShadow1));
    }

    @Test
    void GIVEN_null_request_WHEN_added_THEN_throws() {
        assertThrows(NullPointerException.class, () -> queue.put(null));
        assertThrows(NullPointerException.class, () -> queue.offer(null));
        assertThrows(NullPointerException.class, () -> queue.offer(null, WAIT_SECONDS, TimeUnit.SECONDS));
    }

    @Test
    void GIVEN_use_constructor_without_capacity_THEN_default_capacity_used() {
        RequestBlockingQueue q = new RequestBlockingQueue(merger);
        assertThat(q.remainingCapacity(), is(RequestBlockingQueue.MAX_CAPACITY));
    }

    @Test
    void GIVEN_item_WHEN_remove_THEN_item_removed() {
        queue.offer(thingAShadow1);
        assertThat("queue empty", queue.isEmpty(), is(false));

        queue.remove(thingAShadow2);
        assertThat("queue empty", queue.isEmpty(), is(false));

        queue.remove(thingAShadow1);
        assertThat("queue empty", queue.isEmpty(), is(true));
    }

    @Test
    void GIVEN_empty_queue_WHEN_offerAndTake_THEN_return_offered() {
        assertThat(queue.offerAndTake(thingAShadow1, true), is(thingAShadow1));
    }

    @Test
    void GIVEN_non_empty_queue_WHEN_offerAndTake_THEN_return_head() {
        queue.offer(thingAShadow2);
        assertThat(queue.offerAndTake(thingAShadow1, true), is(thingAShadow2));
        assertThat(queue.poll(), is(thingAShadow1));
    }

    @Test
    void GIVEN_non_empty_queue_WHEN_offerAndTake_same_shadow_new_THEN_return_merged() {
        queue.offer(thingAShadow1);
        when(merger.merge(thingAShadow1, thingAShadow1Again)).thenReturn(thingAShadow1Merged);
        assertThat(queue.offerAndTake(thingAShadow1Again, true), is(thingAShadow1Merged));
        assertThat("queue empty", queue.isEmpty(), is(true));
    }

    @Test
    void GIVEN_non_empty_queue_WHEN_offerAndTake_same_shadow_old_THEN_return_merged() {
        queue.offer(thingAShadow1);
        when(merger.merge(thingAShadow1Again, thingAShadow1)).thenReturn(thingAShadow1Merged);
        assertThat(queue.offerAndTake(thingAShadow1Again, false), is(thingAShadow1Merged));
        assertThat("queue empty", queue.isEmpty(), is(true));
    }

    @Test
    void GIVEN_non_empty_queue_WHEN_offerAndTake_same_shadow_THEN_merge_and_return_head() {
        queue.offer(thingAShadow2);
        queue.offer(thingAShadow1);
        when(merger.merge(thingAShadow1Again, thingAShadow1)).thenReturn(thingAShadow1Merged);
        assertThat(queue.offerAndTake(thingAShadow1Again, false), is(thingAShadow2));
        assertThat(queue.poll(), is(thingAShadow1Merged));
        assertThat("queue empty", queue.isEmpty(), is(true));
    }
}
