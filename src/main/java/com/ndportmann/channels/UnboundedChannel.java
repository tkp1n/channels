package com.ndportmann.channels;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executor;

final class UnboundedChannel<T> extends Channel<T> implements ChannelReader<T>, ChannelWriter<T>  {
    private final BlockedReaderDeque<T> blockedReaders = new BlockedReaderDeque<>();

    UnboundedChannel() {
        this(null, null, true);
    }

    UnboundedChannel(Executor readerExecutor, Executor writerExecutor) {
        this(readerExecutor, writerExecutor, false);
    }

    private UnboundedChannel(Executor readerExecutor, Executor writerExecutor, boolean allowSynchronousContinuations) {
        super(new ConcurrentLinkedDeque<>(), readerExecutor, writerExecutor, allowSynchronousContinuations);
    }

    /* ---- ChannelReader ---- */

    @Override
    public T poll() {
        final T item = queue.poll();

        if (item != null && volatileIsChannelCompleted()) {
            completeCompletion();
        }

        return item;
    }

    @Override
    public CompletionStage<T> read() {
        final boolean complete;
        final CompletableFuture<T> future;

        final T item = queue.poll();
        if (item != null) {
            if (volatileIsChannelCompleted()) {
                completeCompletion();
            }
            return CompletableFuture.completedFuture(item);
        }

        synchronized (lock()) {
            final T lItem = queue.poll();
            if (lItem != null) {
                complete = isChannelCompleted();
                // complete synchronously
                future = CompletableFuture.completedFuture(lItem);
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
    public boolean offer(T item) {
        Objects.requireNonNull(item);

        synchronized (lock()) {
            if (doneWriting()) {
                return false;
            }

            if (queue.isEmpty()) {
                // No item in queue -> we may have a blocked reader
                if (blockedReaders.readOne(item)) {
                    return true;
                }
            }

            // No blocked reader -> queue item for later and we're done
            queue.add(item);
            return true;
        }
    }

    @Override
    public CompletionStage<Void> write(T item) {
        Objects.requireNonNull(item);

        synchronized (lock()) {
            if (doneWriting()) {
                return channelClosedFuture();
            }

            if (queue.isEmpty()) {
                // No item in queue -> we may have a blocked reader
                if (blockedReaders.readOne(item)) {
                    return COMPLETED_VOID_FUTURE;
                }
            }

            // No blocked reader -> queue item for later and we're done
            queue.add(item);
            return COMPLETED_VOID_FUTURE;
        }
    }

    @Override
    public boolean tryComplete(Throwable error) {
        final boolean complete;
        synchronized (lock()) {
            if (doneWriting()) {
                return false;
            }

            doneWriting = error == null ? SUCCESSFUL_COMPLETION_SENTINEL : error;
            channelClosed = CompletableFuture.failedFuture(doneWriting);
            blockedReaders.completeAll();

            complete = queue.isEmpty();
        }

        if (complete) {
            completeCompletion();
        }

        return true;
    }
}
