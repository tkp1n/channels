package com.ndportmann.channels;

import java.util.ArrayDeque;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

final class BoundedChannel<T> extends Channel<T> implements ChannelReader<T>, ChannelWriter<T> {
    private final int capacity;
    private final BoundedChannelFullMode fullMode;
    private final BlockedReaderDeque<T> blockedReaders = new BlockedReaderDeque<>();
    private final BlockedWriterDeque blockedWriters = new BlockedWriterDeque();

    BoundedChannel(final int capacity, final BoundedChannelFullMode fullMode) {
        this(capacity, fullMode, null, null, true);
    }

    BoundedChannel(final int capacity, final BoundedChannelFullMode fullMode, final Executor readerExecutor, final Executor writerExecutor) {
        this(capacity, fullMode, readerExecutor, writerExecutor, false);
    }

    private BoundedChannel(final int capacity, final BoundedChannelFullMode fullMode, final Executor readerExecutor, final Executor writerExecutor, final boolean allowSynchronousContinuations) {
        super(new ArrayDeque<>(verifyQueueCapacity(capacity)), readerExecutor, writerExecutor, allowSynchronousContinuations);
        if (!allowSynchronousContinuations) {
            Objects.requireNonNull(readerExecutor);
            Objects.requireNonNull(writerExecutor);
        }

        this.capacity = capacity;
        this.fullMode = fullMode;
    }

    private static int verifyQueueCapacity(int capacity) {
        if (capacity < 1) throw new IllegalArgumentException("capacity");
        return capacity;
    }

    /* ---- ChannelReader ---- */

    @Override
    public T poll() {
        final T item;
        final boolean complete;

        synchronized (lock()) {
            item = queue.poll();

            if (item != null) {
                // We received an item -> space for another item to be written
                blockedWriters.writeOne();

                complete = isChannelCompleted();
            } else {
                // If queue is empty, there should be no pending writes
                assert blockedWriters.isEmpty();
                return null;
            }
        }

        if (complete) {
            completeCompletion();
        }

        return item; // maybe null
    }

    @Override
    public CompletionStage<T> read() {
        final boolean complete;
        final CompletableFuture<T> future;

        synchronized (lock()) {
            final T item = queue.poll();
            if (item != null) {
                // We received an item -> space for another item to be written
                blockedWriters.writeOne();

                complete = isChannelCompleted();
                // complete synchronously
                future = CompletableFuture.completedFuture(item);
            } else {
                if (doneWriting()) {
                    return channelClosed;
                }

                return blockedReaders.registerNewReader(this);
            }
        }

        if (complete) {
            completeCompletion();
        }

        return future;
    }

    /* ---- ChannelWriter ---- */

    @Override
    public boolean offer(final T item) {
        Objects.requireNonNull(item);

        synchronized (lock()) {
            if (doneWriting()) {
                return false;
            }

            final int size = queue.size();
            if (size == 0) {
                // No item in queue -> we may have a blocked reader
                if (!blockedReaders.readOne(item)) {
                    // No blocked reader -> queue item for later and we're done
                    queue.add(item);
                }

                return true;
            } else if (size < capacity) {
                // No blocked reader -> queue item for later and we're done
                queue.add(item);
                return true;
            }

            // Handle write synchronously if possible
            return fullMode.handleWriteOnBufferFull(item, queue);
        }
    }

    @Override
    public CompletionStage<Void> write(final T item) {
        Objects.requireNonNull(item);

        synchronized (lock()) {
            if (doneWriting()) {
                return channelClosedFuture();
            }

            final int size = queue.size();
            if (size == 0) {
                // No item in queue -> we may have a blocked reader
                if (!blockedReaders.readOne(item)) {
                    // No blocked reader -> queue item for later and we're done
                    queue.add(item);
                }

                return COMPLETED_VOID_FUTURE;
            } else if (size < capacity) {
                // No blocked reader -> queue item for later and we're done
                queue.add(item);
                return COMPLETED_VOID_FUTURE;
            }

            if (fullMode.handleWriteOnBufferFull(item, queue)) {
                // write could be handled synchronously
                return COMPLETED_VOID_FUTURE;
            }

            assert fullMode == BoundedChannelFullMode.WAIT;

            // return future to async write operation
            return blockedWriters.registerNewWriter(this, item);
        }
    }

    @Override
    public boolean tryComplete(final Throwable error) {
        final boolean complete;
        synchronized (lock()) {
            if (doneWriting()) {
                return false;
            }

            doneWriting = error == null ? SUCCESSFUL_COMPLETION_SENTINEL : error;
            channelClosed = CompletableFuture.failedFuture(doneWriting);
            blockedWriters.completeAll();
            blockedReaders.completeAll();

            complete = queue.isEmpty();
        }

        if (complete) {
            completeCompletion();
        }

        return true;
    }
}
