package com.ndportmann.channels;

import java.util.Deque;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

public abstract class Channel<T> extends DualChannel<T, T> implements ChannelReader<T>, ChannelWriter<T> {
    static final Throwable SUCCESSFUL_COMPLETION_SENTINEL = new ChannelClosedException();
    static final CompletableFuture<Void> COMPLETED_VOID_FUTURE = CompletableFuture.completedFuture(null);

    private final Executor readerExecutor;
    private final Executor writerExecutor;
    private final boolean allowSynchronousContinuations;
    private final CompletableFuture<Void> completion = new CompletableFuture<>();

    final Deque<T> queue;

    Throwable doneWriting = null;
    CompletableFuture<T> channelClosed = null;

    Channel(Deque<T> queue, Executor readerExecutor, Executor writerExecutor, boolean allowSynchronousContinuations) {
        if (!allowSynchronousContinuations) {
            Objects.requireNonNull(readerExecutor);
            Objects.requireNonNull(writerExecutor);
        }

        this.queue = queue;
        this.readerExecutor = readerExecutor;
        this.writerExecutor = writerExecutor;
        this.allowSynchronousContinuations = allowSynchronousContinuations;
    }

    @Override
    public final ChannelReader<T> reader() {
        return this;
    }

    @Override
    public final ChannelWriter<T> writer() {
        return this;
    }

    @Override
    public final CompletionStage<Void> completion() {
        return completion;
    }

    final Object lock() {
        return queue;
    }

    final boolean isWritable() {
        // Not having a Throwable stored in doneWriting is used to indicate whether the channel is writable.
        return doneWriting == null;
    }

    final boolean doneWriting() {
        return doneWriting != null;
    }

    final boolean isChannelCompleted() {
        assert Thread.holdsLock(lock());

        return doneWriting() && queue.isEmpty();
    }

    final void completeCompletion() {
        assert !Thread.holdsLock(lock());

        if (doneWriting == SUCCESSFUL_COMPLETION_SENTINEL) {
            completion.complete(null);
        } else {
            completion.completeExceptionally(doneWriting);
        }
    }

    final ReadOperation newReadOperation() {
        return new ReadOperation();
    }

    final WriteOperation newWriteOperation(T item) {
        return new WriteOperation(item);
    }

    @SuppressWarnings("unchecked")
    final CompletableFuture<Void> channelClosedFuture() {
        return (CompletableFuture<Void>) channelClosed;
    }

    abstract class AsyncOperation<U> extends MinimalFuture<U> {
        final void close() {
            assert Thread.holdsLock(lock());
            assert doneWriting();

            if (allowSynchronousContinuations) {
                completeExceptionally();
            } else {
                writerExecutor.execute(this::completeExceptionally);
            }
        }

        private void completeExceptionally() {
            final var success = completeExceptionallyInternal(doneWriting);
            assert success;
        }
    }

    final class ReadOperation extends AsyncOperation<T> {
        private T item;

        final void read(final T item) {
            this.item = item;

            if (allowSynchronousContinuations) {
                read0();
            } else {
                readerExecutor.execute(this::read0);
            }
        }

        final void read0() {
            final var success = completeInternal(item);
            assert success;
        }
    }

    final class WriteOperation extends AsyncOperation<Void> {
        private final T item;

        WriteOperation(final T item) {
            this.item = item;
        }

        final void write() {
            assert Thread.holdsLock(lock());
            assert isWritable();

            queue.add(item);

            if (allowSynchronousContinuations) {
                complete();
            } else {
                writerExecutor.execute(this::complete);
            }
        }

        final void complete() {
            final var success = completeInternal(null);
            assert success;
        }
    }

    //<editor-fold desc="Static members">

    public static BoundedChannelBuilder createBounded(int capacity) {
        return new BoundedChannelBuilder(capacity);
    }

    public static UnboundedChannelBuilder createUnbounded() {
        return new UnboundedChannelBuilder();
    }

    @SuppressWarnings("unchecked")
    private abstract static class ChannelBuilder<T extends ChannelBuilder<T>> {
        boolean singleWriter = false;
        boolean singleReader = false;
        Executor writerExecutor = ForkJoinPool.commonPool();
        Executor readerExecutor = ForkJoinPool.commonPool();
        boolean allowSynchronousContinuations = false;

        public final T singleWriter() {
            singleWriter = true;
            return (T)this;
        }

        public final T singleReader() {
            singleReader = true;
            return (T)this;
        }

        public final T scheduleWriterContinuationsOn(Executor writerExecutor) {
            this.writerExecutor = writerExecutor;
            return (T)this;
        }

        public final T scheduleReaderContinuationsOn(Executor readerExecutor) {
            this.readerExecutor = readerExecutor;
            return (T)this;
        }

        public final T allowSynchronousContinuations() {
            this.allowSynchronousContinuations = true;
            return (T)this;
        }
    }

    public final static class BoundedChannelBuilder extends ChannelBuilder<BoundedChannelBuilder> {
        private final int capacity;
        private BoundedChannelFullMode fullMode = BoundedChannelFullMode.WAIT;

        public BoundedChannelBuilder(int capacity) {
            this.capacity = capacity;
        }

        public final BoundedChannelBuilder fullMode(BoundedChannelFullMode fullMode) {
            this.fullMode = fullMode;
            return this;
        }

        public final <T> Channel<T> build() {
            if (allowSynchronousContinuations) {
                return new BoundedChannel<>(capacity, fullMode);
            } else {
                return new BoundedChannel<>(capacity, fullMode, readerExecutor, writerExecutor);
            }
        }
    }

    public final static class UnboundedChannelBuilder extends ChannelBuilder<UnboundedChannelBuilder> {
        public final <T> Channel<T> build() {
            if (allowSynchronousContinuations) {
                return new UnboundedChannel<>();
            } else {
                return new UnboundedChannel<>(readerExecutor, writerExecutor);
            }
        }
    }
    //</editor-fold>
}
