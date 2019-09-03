package com.ndportmann.channels;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Deque;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

public abstract class Channel<T> extends DualChannel<T, T> implements ChannelReader<T>, ChannelWriter<T> {
    private static final VarHandle DONE_WRITING;
    static final Throwable SUCCESSFUL_COMPLETION_SENTINEL = new ChannelClosedException();
    static final CompletableFuture<Void> COMPLETED_VOID_FUTURE = CompletableFuture.completedFuture(null);

    static {
        try {
            DONE_WRITING = MethodHandles
                    .privateLookupIn(Channel.class, MethodHandles.lookup())
                    .findVarHandle(Channel.class, "doneWriting", Throwable.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new IllegalStateException(e);
        }
    }

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

    /** {@inheritDoc} */
    @Override
    public final ChannelReader<T> reader() {
        return this;
    }

    /** {@inheritDoc} */
    @Override
    public final ChannelWriter<T> writer() {
        return this;
    }

    /** {@inheritDoc} */
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

    final boolean volatileDoneWriting() {
        return ((Throwable)DONE_WRITING.getVolatile(this)) != null;
    }

    final boolean isChannelCompleted() {
        assert Thread.holdsLock(lock());

        return doneWriting() && queue.isEmpty();
    }

    final boolean volatileIsChannelCompleted() {
        assert !Thread.holdsLock(lock());

        return volatileDoneWriting() && queue.isEmpty();
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

    /**
     * Creates a builder for a {@link Channel} with the specified maximum capacity.
     *
     * @param capacity The maximum number of items the channel may store.
     * @return A builder for the channel.
     */
    public static BoundedChannelBuilder createBounded(int capacity) {
        return new BoundedChannelBuilder(capacity);
    }

    /**
     * Creates a builder for an unbounded {@link Channel}.
     *
     * @return A builder for the channel.
     */
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

        /**
         * Optimizes the {@link Channel} for situations where there will only ever be
         * at most one read operation at a time.
         *
         * @return This builder for the channel.
         */
        public final T singleWriter() {
            singleWriter = true;
            return (T)this;
        }

        /**
         * Optimizes the {@link Channel} for situations where there will only ever be
         * at most one write operation at a time.
         *
         * @return This builder for the channel.
         */
        public final T singleReader() {
            singleReader = true;
            return (T)this;
        }

        /**
         * Sets the {@link Executor} to be used by the {@link Channel} to invoke continuations
         * subscribed to notifications of pending write operations.
         *
         * @param writerExecutor The {@link Executor} to be used by the {@link Channel} to invoke
         * continuations subscribed to notifications of pending write operations.
         * @return This builder for the channel.
         */
        public final T scheduleWriterContinuationsOn(Executor writerExecutor) {
            this.writerExecutor = writerExecutor;
            return (T)this;
        }

        /**
         * Sets the {@link Executor} to be used by the {@link Channel} to invoke continuations
         * subscribed to notifications of pending read operations.
         *
         * @param readerExecutor The {@link Executor} to be used by the {@link Channel} to invoke
         * continuations subscribed to notifications of pending read operations.
         * @return This builder for the channel.
         */
        public final T scheduleReaderContinuationsOn(Executor readerExecutor) {
            this.readerExecutor = readerExecutor;
            return (T)this;
        }

        /**
         * Allows the {@link Channel} to invoke continuations subscribed to notifications
         * of pending async operations synchronously.
         *
         * @return This builder for the channel.
         */
        public final T allowSynchronousContinuations() {
            this.allowSynchronousContinuations = true;
            return (T)this;
        }
    }

    public final static class BoundedChannelBuilder extends ChannelBuilder<BoundedChannelBuilder> {
        final int capacity;
        BoundedChannelFullMode fullMode = BoundedChannelFullMode.WAIT;

        BoundedChannelBuilder(int capacity) {
            this.capacity = capacity;
        }

        /**
         * Sets the behavior incurred by write operations when the channel is full.
         *
         * @param fullMode The behavior incurred by write operations when the channel is full.
         * @return This builder for the channel.
         */
        public final BoundedChannelBuilder fullMode(BoundedChannelFullMode fullMode) {
            this.fullMode = fullMode;
            return this;
        }

        /**
         * Returns the configured {@link Channel}.
         *
         * @param <T> Specifies the type of data in the channel.
         * @return The configured {@link Channel}.
         */
        public final <T> Channel<T> build() {
            if (allowSynchronousContinuations) {
                return new BoundedChannel<>(capacity, fullMode);
            } else {
                return new BoundedChannel<>(capacity, fullMode, readerExecutor, writerExecutor);
            }
        }
    }

    public final static class UnboundedChannelBuilder extends ChannelBuilder<UnboundedChannelBuilder> {
        /**
         * Returns the configured {@link Channel}.
         *
         * @param <T> Specifies the type of data in the channel.
         * @return The configured {@link Channel}.
         */
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
