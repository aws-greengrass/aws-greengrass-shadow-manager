/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync;

import com.aws.greengrass.shadowmanager.sync.model.SyncRequest;

import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

/**
 * Blocking queue implementation that keeps a single request per shadow. If a request comes in for a shadow, it is
 * merged together with the existing request. Thread safe.
 */
public class RequestBlockingQueue extends AbstractQueue<SyncRequest> implements BlockingQueue<SyncRequest> {

    private final Function<? super SyncRequest, Object> keyMapper;
    private final Map<Object, SyncRequest> requests;
    private final BiFunction<? super SyncRequest, ? super SyncRequest, ? extends SyncRequest> merger;
    private final BlockingQueue<Object> queue;

    // Lock for removing items from the queue - this ensures that drain does not end up waiting forever due to
    // simultaneous removals
    private final ReentrantLock takeLock = new ReentrantLock();

    public RequestBlockingQueue(Function<? super SyncRequest, Object> keyMapper,
            BiFunction<? super SyncRequest, ? super SyncRequest, ? extends SyncRequest> merger) {
        this(keyMapper, merger, new ConcurrentHashMap<>(), new LinkedBlockingQueue<>());
    }

    RequestBlockingQueue(Function<? super SyncRequest, Object> keyMapper,
            BiFunction<? super SyncRequest, ? super SyncRequest, ? extends SyncRequest> merger,
            Map<Object, SyncRequest> requests, BlockingQueue<Object> queue) {
        super();
        this.keyMapper = keyMapper;
        this.merger = merger;
        this.requests = requests;
        this.queue = queue;

    }

    @Override
    public void put(SyncRequest value) throws InterruptedException {
        unwrap(() -> enqueueRequest(value, k -> {
            queue.put(k);
            return true; // put will always succeed as it cannot timeout (but it can be interrupted)
        }));
    }

    @Override
    public boolean offer(SyncRequest value, long timeout, TimeUnit unit) throws InterruptedException {
        return unwrap(() -> enqueueRequest(value, k -> queue.offer(k, timeout, unit)));
    }

    @Override
    public boolean offer(SyncRequest value) {
        // offer does not throw InterruptedException so there is no need to unwrap
        return enqueueRequest(value, queue::offer);
    }

    @Override
    public SyncRequest take() throws InterruptedException {
        takeLock.lockInterruptibly();
        try {
            // take waits until an object is available
            Object key = queue.take();
            return requests.remove(key);
        } finally {
            takeLock.unlock();
        }
    }

    @Override
    public SyncRequest poll(long timeout, TimeUnit unit) throws InterruptedException {
        takeLock.lockInterruptibly();
        try {
            Object key = queue.poll(timeout, unit);
            if (key == null) {
                // key is null if poll times out
                return null;
            }
            return requests.remove(key);
        } finally {
            takeLock.unlock();
        }
    }

    @Override
    public SyncRequest poll() {
        takeLock.lock();
        try {
            final Object key = queue.poll();
            if (key == null) {
                // key is null if queue is empty
                return null;
            }
            return requests.remove(key);
        } finally {
            takeLock.unlock();
        }
    }

    @Override
    public SyncRequest peek() {
        takeLock.lock();
        try {
            final Object key = queue.peek();
            if (key == null) {
                // key is null if queue is empty
                return null;
            }
            return requests.get(key);
        } finally {
            takeLock.unlock();
        }
    }

    @Override
    public int drainTo(Collection<? super SyncRequest> c) {
        return drainTo(c, Integer.MAX_VALUE);
    }

    @Override
    public int drainTo(Collection<? super SyncRequest> c, int maxElements) {
        // This collection can have multiple requests for the same shadow. Once a request is removed, it is fair
        // to add a new request for the same shadow.
        takeLock.lock();
        try {
            final int n = Math.min(maxElements, size());
            for (int i = 0; i < n; i++) {
                c.add(poll());
            }
            return n;
        } finally {
            takeLock.unlock();
        }
    }

    @Override
    public int size() {
        return queue.size();
    }

    @Override
    public int remainingCapacity() {
        return queue.remainingCapacity();
    }

    @Override
    public Iterator<SyncRequest> iterator() {
        return new QueueIterator();
    }

    private class QueueIterator implements Iterator<SyncRequest> {
        private SyncRequest next = null;
        Iterator<Object> keys;

        public QueueIterator() {
            takeLock.lock();
            try {
                // weak iterator with no guarantees that objects are not removed or added while iterating
                keys = queue.iterator();
                if (keys.hasNext()) {
                    Object key = keys.next();
                    next = requests.get(key);
                }
            } finally {
                takeLock.unlock();
            }
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @SuppressWarnings("PMD.NullAssignment")
        @Override
        public SyncRequest next() {
            if (next == null) {
                throw new NoSuchElementException();
            }
            takeLock.lock();
            SyncRequest ret = next;
            try {
                next = null;
                if (keys.hasNext()) {
                    next = requests.get(keys.next());
                }
                return ret;
            } finally {
                takeLock.unlock();
            }
        }
    }


    /**
     * Interface for an interruptable function to add an object to a queue.
     */
    @FunctionalInterface
    private interface Queuer {
        boolean enqueue(Object o) throws InterruptedException;
    }

    /**
     * Enqueue a request into the queue and request map.
     *
     * @param value the value to enqueue.
     * @param queuer an operation for enqueuing. This may throw an InterruptedException.
     * @return true if the value is enqueued.
     * @throws WrappedInterruptedException if an InterruptedException occurs while adding the key to the backing queue
     */
    private boolean enqueueRequest(SyncRequest value, Queuer queuer) throws WrappedInterruptedException {
        final Object key = keyMapper.apply(value);

        // Compute a new value for the map by either storing the request if one is not present, or merging if it does.
        // The key is added to the queue first and if that succeeds, then the map can be updated
        // `compute` is atomic for `ConcurrentHashMap` and will synchronize the map so concurrent changes cannot occur
        SyncRequest computed = requests.compute(key, (k, oldValue) -> {
            if (oldValue != null) {
                return merger.apply(oldValue, value);
            }
            try {
                if (queuer.enqueue(k)) {
                    // if key is enqueued the new `value` can be used in the map
                    return value;
                } else {
                    // return null to indicate nothing will be added to the map
                    return null;
                }
            } catch (InterruptedException e) {
                throw new WrappedInterruptedException(e);
            }
        });

        // computed will be null if the `enqueue` operation fails
        return computed != null;
    }

    /**
     * Utility to rethrow a wrapped {@link InterruptedException}.
     *
     * @param s a supplier - the enqueue function returns a boolean if it succeeds
     * @return the result from the supplied function
     * @throws InterruptedException if a {@link WrappedInterruptedException} is thrown from the supplier
     */
    private boolean unwrap(BooleanSupplier s) throws InterruptedException {
        try {
            return s.getAsBoolean();
        } catch (WrappedInterruptedException e) {
            throw e.getCause();
        }
    }

    static class WrappedInterruptedException extends RuntimeException {

        private static final long serialVersionUID = 5805388348668990285L;

        public WrappedInterruptedException(InterruptedException cause) {
            super(cause);
        }

        @Override
        public synchronized InterruptedException getCause() {
            return (InterruptedException) super.getCause();
        }
    }

}
