/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync;

import com.aws.greengrass.shadowmanager.sync.model.SyncRequest;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import javax.inject.Inject;

import static com.aws.greengrass.shadowmanager.model.Constants.DEFAULT_SHADOW_DOCUMENTS_SYNCED;

/**
 * Blocking "queue" implementation that keeps a single request per shadow. If a request comes in for a shadow, it is
 * merged together with the existing request.
 * <p/>
 * This class is thread safe and implements the useful methods of the {@link java.util.concurrent.BlockingQueue}
 * interface. However, this class intentionally does not implement the interface as it is not a Collection. It is not
 * iterable as the iterator needs to be synchronized to avoid concurrent modifications. This is not possible with the
 * current implementation (but storing keys in a separate linked list could be used to solve this).
 * <p/>
 * For the simplest blocking use case, threads can call {@link #take()} and they will wait until items are available.
 * Threads can call {@link #put(SyncRequest)} and they will wait until space is available to insert.
 */
public class RequestBlockingQueue {

    /**
     * Default maximum number of queued requests.
     */
    private final RequestMerger merger;
    private final Map<String, SyncRequest> requests;

    /**
     * Lock for ensuring synchronous access to request map.
     */
    private final ReentrantLock lock = new ReentrantLock();
    /**
     * Condition to signal threads waiting for queue to contain data.
     */
    private final Condition notEmpty = lock.newCondition();
    /**
     * Condition to signal threads waiting for queue to have space for more data.
     */
    private final Condition notFull = lock.newCondition();
    /**
     * Capacity of queue.
     */
    private int capacity;

    /**
     * Create a new instance with a capacity of 1024 (DEFAULT_SHADOW_DOCUMENTS_SYNCED).
     *
     * @param merger a merger
     */
    @Inject
    public RequestBlockingQueue(RequestMerger merger) {
        this(merger, DEFAULT_SHADOW_DOCUMENTS_SYNCED);
    }

    /**
     * Create a new instance with a capacity.
     *
     * @param merger   a merger
     * @param capacity the max capacity
     */
    public RequestBlockingQueue(RequestMerger merger, int capacity) {
        this(merger, capacity, new LinkedHashMap<>());
    }

    RequestBlockingQueue(RequestMerger merger, int capacity, Map<String, SyncRequest> requests) {
        super();
        this.merger = merger;
        this.requests = requests;
        this.capacity = capacity;
    }

    /**
     * Create a key from the request.
     *
     * @param value the request.
     * @return the key.
     */
    private String createKey(SyncRequest value) {
        return value.getThingName() + "|" + value.getShadowName();
    }

    /**
     * Add a request to the queue.
     *
     * @param value the item to add.
     */
    private void enqueue(SyncRequest value) {
        requests.put(createKey(value), value);
    }

    /**
     * Remove a request from the queue.
     *
     * @return the removed item
     */
    private SyncRequest dequeue() {
        String key = this.requests.keySet().iterator().next();
        return this.requests.remove(key);
    }

    /**
     * Tell thread to wait until the thread is not full.
     */
    private void waitIfFull() throws InterruptedException {
        while (isFull()) {
            notFull.await();
        }
    }

    /**
     * Tell thread to wait until the thread is not empty.
     */
    private void waitIfEmpty() throws InterruptedException {
        while (isEmpty()) {
            notEmpty.await();
        }
    }

    /**
     * Signal waiting threads if the queue is not full.
     */
    private void signalIfNotFull() {
        if (!isFull()) {
            notFull.signal();
        }
    }

    /**
     * Signal waiting threads if the queue is not empty.
     */
    private void signalIfNotEmpty() {
        if (!isEmpty()) {
            notEmpty.signal();
        }
    }

    /**
     * While a criteria is true, wait for the time specified.
     *
     * @param criteria  the supplied value is the criteria which will cause the current thread to wait
     * @param condition a condition to wait on. This method assumes the associated lock is held by the current thread
     * @param timeout   how long to wait, before giving up, in terms of unit
     * @param unit      how to interpret the timeout
     * @return true if the timeout has not expired, otherwise false
     * @throws InterruptedException if the current thread is interrupted while waiting
     */
    private boolean await(BooleanSupplier criteria, Condition condition, long timeout, TimeUnit unit)
            throws InterruptedException {
        long remaining = unit.toNanos(timeout);
        while (criteria.getAsBoolean()) {
            if (remaining <= 0L) {
                return false;
            }
            remaining = condition.awaitNanos(remaining);
        }
        return true;
    }

    /**
     * Merge the request with an existing request in the queue. This only updates if a request for the shadow is already
     * present in the queue. This treats the value being offered as 'new' when merging.
     *
     * @param value the request to merge
     * @return true if the request was merged; false when no existing request is present.
     */
    private boolean mergeIfPresent(SyncRequest value) {
        return mergeIfPresent(value, true);
    }

    /**
     * Merge the request with an existing request in the queue. This only updates if a request for the shadow is already
     * present in the queue.
     *
     * @param value the request to merge
     * @param isNewValue true if the value to merge should be merged on top of the existing value. When false, the
     *                   offered value is treated as the base and the current value in the map is merged on top
     * @return true if the request was merged; false when no existing request is present.
     */
    private boolean mergeIfPresent(SyncRequest value, boolean isNewValue) {
        String key = createKey(value);
        SyncRequest updated = requests.computeIfPresent(key, (k, currentValue) -> {
            if (isNewValue) {
                return merger.merge(currentValue, value);
            }
            return merger.merge(value, currentValue);
        });
        return updated != null;
    }

    /**
     * Add an item to the queue. This will wait if the queue is full until there is space. If a request for the
     * shadow already exists in the queue, it is merged and no extra capacity is consumed.
     *
     * @param value a value to add
     * @throws InterruptedException if the thread is interrupted while waiting
     * @throws NullPointerException if value is null
     */
    @SuppressWarnings("PMD.AvoidThrowingNullPointerException") // BlockingQueue Interface requires NPE on null
    public void put(SyncRequest value) throws InterruptedException {
        if (value == null) {
            throw new NullPointerException();
        }
        lock.lockInterruptibly();
        try {
            if (!mergeIfPresent(value)) {
                waitIfFull();

                enqueue(value);
            }
            signalIfNotFull();
            signalIfNotEmpty();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Take the head of the queue and add an item in one atomic action. If the queue is empty, the given item is
     * returned.
     *
     * @param value a request to add
     * @param isNewValue whether the value being offered should be considered as new. When offering back an "old"
     *                   request to retry, this should be set to false so that a request in the queue is merged
     *                   correctly.
     * @return the head of the queue (which may be the given value if the queue is empty)
     * @throws NullPointerException if value is null
     */
    @SuppressWarnings("PMD.AvoidThrowingNullPointerException")
    public SyncRequest offerAndTake(SyncRequest value, boolean isNewValue) {
        if (value == null) {
            throw new NullPointerException();
        }
        lock.lock();
        try {
            if (isEmpty()) {
                return value;
            }
            boolean merged = mergeIfPresent(value, isNewValue);
            SyncRequest head = dequeue();
            if (merged) {
                return head;
            }
            enqueue(value);
            // no signalling as we are not changing "fullness" of the queue
            return head;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Add an item to the queue, with a timeout to wait while the queue is at capacity.  If a request for the
     * shadow already exists in the queue, it is merged and no extra capacity is consumed.
     *
     * @param value   a value to add
     * @param timeout how long to wait, before giving up, in terms of unit
     * @param unit    how to interpret the timeout
     * @return true if the item was added; false if the timeout expired
     * @throws InterruptedException if the thread is interrupted while waiting
     * @throws NullPointerException if value is null
     */
    @SuppressWarnings("PMD.AvoidThrowingNullPointerException") // BlockingQueue Interface requires NPE on null
    public boolean offer(SyncRequest value, long timeout, TimeUnit unit) throws InterruptedException {
        if (value == null) {
            throw new NullPointerException();
        }

        lock.lockInterruptibly();
        try {
            if (!mergeIfPresent(value)) {
                if (!await(this::isFull, notFull, timeout, unit)) {
                    return false;
                }
                enqueue(value);
            }
            signalIfNotFull();
            signalIfNotEmpty();
        } finally {
            lock.unlock();
        }
        return true;
    }

    /**
     * Add an item to the queue. If the queue has no space, return immediately.  If a request for the
     * shadow already exists in the queue, it is merged and no extra capacity is consumed.
     *
     * @param value the value to add
     * @return true if the item is added, otherwise false.
     * @throws NullPointerException if value is null
     */
    @SuppressWarnings("PMD.AvoidThrowingNullPointerException") // BlockingQueue Interface requires NPE on null
    public boolean offer(SyncRequest value) {
        if (value == null) {
            throw new NullPointerException();
        }
        lock.lock();
        try {
            if (!mergeIfPresent(value)) {
                if (isFull()) {
                    return false;
                } else {
                    enqueue(value);
                }
            }
            signalIfNotFull();
            signalIfNotEmpty();
        } finally {
            lock.unlock();
        }
        return true;
    }

    /**
     * Take an item from the queue. This will wait indefinitely for an item to become available.
     *
     * @return the request from the queue
     * @throws InterruptedException if the thread is interrupted while waiting for data
     */
    public SyncRequest take() throws InterruptedException {
        lock.lockInterruptibly();
        try {
            waitIfEmpty();
            SyncRequest value = dequeue();
            signalIfNotFull();
            signalIfNotEmpty();
            return value;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Take an item from the queue. This waits for up to the specified wait time for an item to become available.
     *
     * @param timeout how long to wait, before giving up, in terms of unit
     * @param unit    how to interpret the timeout
     * @return the request at the front of the queue or null if the timeout expired
     * @throws InterruptedException if the thread is interrupted while waiting
     */
    public SyncRequest poll(long timeout, TimeUnit unit) throws InterruptedException {
        lock.lockInterruptibly();
        try {
            if (!await(this::isEmpty, notEmpty, timeout, unit)) {
                return null;
            }
            SyncRequest value = dequeue();
            signalIfNotFull();
            signalIfNotEmpty();
            return value;
        } finally {
            lock.unlock();
        }

    }

    /**
     * Take an item from the queue. Returns immediately if no item is available.
     *
     * @return the request at the head of the queue or null if no items are available.
     */
    public SyncRequest poll() {
        lock.lock();
        try {
            if (isEmpty()) {
                return null;
            }
            SyncRequest value = dequeue();
            signalIfNotFull();
            signalIfNotEmpty();
            return value;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Return, but don't remove, the head of the queue.
     *
     * @return the item at the head of the queue, or null if it is empty.
     */
    public SyncRequest peek() {
        lock.lock();
        try {
            return requests.values().stream().findFirst().orElse(null);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Remove the request associated with the shadow. The removed request may not be the same as the value passed in -
     * only the thing name and shadow name are used to determine which request to remove.
     *
     * @param value a request associated with as shadow.
     * @return the removed request
     */
    public SyncRequest remove(SyncRequest value) {
        lock.lock();
        try {
            SyncRequest removed = requests.remove(createKey(value));
            signalIfNotEmpty();
            signalIfNotFull();
            return removed;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns true if this queue has no items.
     *
     * @return true if the queue has no item
     */
    public boolean isEmpty() {
        lock.lock();
        try {
            return requests.isEmpty();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Return true if the queue has no more space.
     *
     * @return true if the queue has no more space
     */
    public boolean isFull() {
        lock.lock();
        try {
            return capacity == requests.size();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Remove all items from the queue.
     */
    public void clear() {
        lock.lock();
        try {
            requests.clear();
            signalIfNotFull();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns the number of items in the queue.
     *
     * @return the number of items in the queue.
     */
    public int size() {
        lock.lock();
        try {
            return requests.size();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns true if the total capacity of the queue could be updated. Returns false if the capacity is less than
     * the current number of requests.
     *
     * @param newCapacity number of requests to queue.
     * @return true if the queue capacity could be updated, false if the desired capacity is too small.
     */
    public boolean updateCapacity(int newCapacity) {
        lock.lock();
        try {
            if (newCapacity > requests.size()) {
                this.capacity = newCapacity;
                return true;
            }
            return false;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns the amount of space left in the queue.
     *
     * @return the amount of space left in the queue.
     */
    public int remainingCapacity() {
        lock.lock();
        try {
            return capacity - requests.size();
        } finally {
            lock.unlock();
        }
    }
}
