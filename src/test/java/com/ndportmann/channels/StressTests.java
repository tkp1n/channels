package com.ndportmann.channels;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static com.ndportmann.channels.BoundedChannelFullMode.*;
import static com.ndportmann.channels.TestHelper.assertThrowsCause;
import static java.util.concurrent.CompletableFuture.runAsync;
import static org.junit.jupiter.api.Assertions.*;

final class StressTests {
    private static final CompletableFuture<Void> CFv = CompletableFuture.completedFuture(null);
    private static final CompletableFuture<Integer> CFi = CompletableFuture.completedFuture(null);
    private static final int MAX_NUMBER_TO_WRITE = 400_000;
    private static final int MAX_THREAD_COUNT = Math.max(2, Runtime.getRuntime().availableProcessors());

    private static class Options {
        private final boolean bounded;
        private final boolean allowSynchronousContinuations;
        private final boolean singleReader;
        private final boolean singleWriter;
        private final int capacity;
        private final BoundedChannelFullMode fullMode;
        private final Supplier<Channel<Integer>> supplier;

        private Options(boolean bounded, boolean allowSynchronousContinuations, boolean singleReader, boolean singleWriter, int capacity, BoundedChannelFullMode fullMode, Supplier<Channel<Integer>> supplier) {
            this.bounded = bounded;
            this.allowSynchronousContinuations = allowSynchronousContinuations;
            this.singleReader = singleReader;
            this.singleWriter = singleWriter;
            this.capacity = capacity;
            this.fullMode = fullMode;
            this.supplier = supplier;
        }

        @Override
        public String toString() {
            return "Options{" +
                    "bounded=" + bounded +
                    ", allowSynchronousContinuations=" + allowSynchronousContinuations +
                    ", singleReader=" + singleReader +
                    ", singleWriter=" + singleWriter +
                    ", capacity=" + capacity +
                    ", fullMode=" + fullMode +
                    '}';
        }
    }

    static List<Options> channels() {
        List<Options> opts = new ArrayList<>();

        // bounded
        for (var capacity : List.of(1, 1000))
        for (var allowSynchronousContinuations : List.of(true, false))
        for (var singleReader : List.of(true, false))
        for (var readerExecutor : List.of(ForkJoinPool.commonPool(), Executors.newSingleThreadExecutor()))
        for (var singleWriter : List.of(true, false))
        for (var writerExecutor : List.of(ForkJoinPool.commonPool(), Executors.newSingleThreadExecutor()))
        for (var fullMode : List.of(DROP_WRITE, DROP_NEWEST, DROP_OLDEST, WAIT)) {
            opts.add(new Options(
                    true, allowSynchronousContinuations, singleReader, singleWriter, capacity, fullMode,
                    () -> {
                        var builder = Channel.createBounded(capacity);
                        builder.allowSynchronousContinuations = allowSynchronousContinuations;
                        builder.singleReader = singleReader;
                        builder.readerExecutor = readerExecutor;
                        builder.singleWriter = singleWriter;
                        builder.writerExecutor = writerExecutor;
                        builder.fullMode = fullMode;
                        return builder.build();
                    }
            ));
        }

        // unbounded
        for (var allowSynchronousContinuations : List.of(true, false))
        for (var singleReader : List.of(true, false))
        for (var readerExecutor : List.of(ForkJoinPool.commonPool(), Executors.newSingleThreadExecutor()))
        for (var singleWriter : List.of(true, false))
        for (var writerExecutor : List.of(ForkJoinPool.commonPool(), Executors.newSingleThreadExecutor())) {
            opts.add(new Options(
                    false, allowSynchronousContinuations, singleReader, singleWriter, Integer.MAX_VALUE, null,
                    () -> {
                        var builder = Channel.createUnbounded();
                        builder.allowSynchronousContinuations = allowSynchronousContinuations;
                        builder.singleReader = singleReader;
                        builder.readerExecutor = readerExecutor;
                        builder.singleWriter = singleWriter;
                        builder.writerExecutor = writerExecutor;
                        return builder.build();
                    }
            ));
        }

        return opts;
    }

    private CompletionStage<Void> write(Channel<Integer> channel, int value) {
        if (channel.offer(value)) {
            return CFv;
        }

        return channel.write(value);
    }

    private CompletionStage<Integer> read(Channel<Integer> channel) {
        if (channel.poll() != null) {
            return CFi;
        }

        return channel.read();
    }

    @ParameterizedTest
    @MethodSource("channels")
    void runStressTests(Options options) throws ExecutionException, InterruptedException {
        var channel = options.supplier.get();
        var reader = channel.reader();
        var writer = channel.writer();
        var shouldReadAllWrittenValues = !options.bounded || options.fullMode == WAIT;

        int readerTasksCount;
        int writerTasksCount;

        if (options.singleReader) {
            readerTasksCount = 1;
            writerTasksCount = options.singleWriter ? 1 : MAX_THREAD_COUNT - 1;
        } else if (options.singleWriter) {
            writerTasksCount = 1;
            readerTasksCount = MAX_THREAD_COUNT - 1;
        } else {
            readerTasksCount = MAX_THREAD_COUNT / 2;
            writerTasksCount = MAX_THREAD_COUNT - readerTasksCount;
        }

        var readCount = new AtomicInteger();

        var futs = new ArrayList<CompletableFuture>();

        for (int i = 0; i < readerTasksCount; i++) {
            var fut = runAsync(() -> {
                assertThrowsCause(ChannelClosedException.class, () -> {
                    while (true) {
                        read(channel).toCompletableFuture().get();
                        readCount.incrementAndGet();
                    }
                });
            });
            futs.add(fut);
        }

        var numberToWriteToQueue = new AtomicInteger(-1);
        var remainingWriters = new AtomicInteger(writerTasksCount);
        for (int i=0; i < writerTasksCount; i++)
        {
            var fut = runAsync(() -> {
                int num = numberToWriteToQueue.incrementAndGet();
                while (num < MAX_NUMBER_TO_WRITE)
                {
                    TestHelper.runOk(write(channel, num));
                    num = numberToWriteToQueue.incrementAndGet();
                }

                if (remainingWriters.decrementAndGet() == 0)
                    writer.complete();
            });
            futs.add(fut);
        }

        CompletableFuture.allOf(futs.toArray(CompletableFuture[]::new))
                .get();

        if (shouldReadAllWrittenValues) {
            assertEquals(MAX_NUMBER_TO_WRITE, readCount.get());
        } else {
            assertTrue(readCount.get() >= 0);
            assertTrue(readCount.get() <= MAX_NUMBER_TO_WRITE);
        }
    }
}
