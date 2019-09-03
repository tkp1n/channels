package com.ndportmann.channels;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Arrays;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static com.ndportmann.channels.TestHelper.*;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(ChannelResolver.class)
abstract class ChannelTestBase {
    protected abstract <T> Channel<T> createChannel();
    Channel<Integer> createIntChannel() {
        return createChannel();
    }

    protected abstract <T> Channel<T> createFullChannel(Supplier<T> values);
    Channel<Integer> createFullIntChannel() {
        return createFullChannel(() -> 42);
    }

    boolean allowSynchronousContinuations = true;
    private boolean requiresSingleReader = false;
    private boolean requiresSingleWriter = false;

    @Test
    void castMatchesReaderWriter(Channel<Integer> channel) {
        ChannelReader<Integer> cr = channel;
        ChannelWriter<Integer> cw = channel;
        assertEquals(cr, channel.reader());
        assertEquals(cw, channel.writer());
    }

    @Test
    void completionIdempotent(Channel<Integer> channel) {
        var completion = channel.completion();
        assertFalse(completion.toCompletableFuture().isDone());

        assertEquals(completion, channel.completion());
        channel.complete();
        assertEquals(completion, channel.completion());

        assertTrue(completion.toCompletableFuture().isDone());
    }

    @Test
    void completeAfterEmptyNoWaitersTriggersCompletion(Channel<Integer> channel) {
        channel.complete();

        var completion = channel.completion().toCompletableFuture();
        assertTrue(completion.isDone());
    }

    @Test
    void completeAfterEmptyWaitingReaderTriggersCompletion(Channel<Integer> channel) throws Exception {
        var future = channel.read().toCompletableFuture();
        channel.complete();

        run(channel::completion);

        assertThrowsCause(ChannelClosedException.class, future::get);
    }

    @Test
    void completeTwiceThrowsInvalidOperationException(Channel<Integer> channel) {
        channel.complete();
        assertThrows(IllegalStateException.class, channel::complete);
    }

    @Test
    void tryCompleteTwiceReturnsTrueThenFalse(Channel<Integer> channel) {
        assertTrue(channel.tryComplete());
        assertFalse(channel.tryComplete());
        assertFalse(channel.tryComplete());
    }

    @Test
    void tryComplete_ErrorsPropagate() throws Exception {
        // success
        var successChannel = createIntChannel();
        assertTrue(successChannel.tryComplete());
        run(successChannel::completion);

        // error
        var errorChannel = createIntChannel();
        assertTrue(errorChannel.tryComplete(new IllegalArgumentException()));
        assertThrowsCause(IllegalArgumentException.class, exec(errorChannel::completion));
    }

    @Test
    void singleProducerConsumerConcurrentReadWriteSuccess(Channel<Integer> channel) throws Exception {
        int numItems = 100000;
        CompletableFuture.allOf(
                CompletableFuture.runAsync(() -> {
                    for (int i = 0; i < numItems; i++) {
                        channel.write(i);
                    }
                }),
                CompletableFuture.runAsync(() -> {
                    for (int i = 0; i < numItems; i++) {
                        try {
                            assertEquals(i, run(channel::read));
                        } catch (Exception ex) {
                            fail(ex);
                        }
                    }
                })
        ).get();
    }

    @Test
    void singleProducerConsumerPingPongSuccess() throws Exception {
        var channel1 = createIntChannel();
        var channel2 = createIntChannel();

        int numItems = 100000;
        CompletableFuture.allOf(
                CompletableFuture.runAsync(() -> {
                    for (int i = 0; i < numItems; i++) {
                        try {
                            assertEquals(i, run(channel1::read));
                        } catch (Exception ex) {
                            fail(ex);
                        }
                        channel2.write(i);
                    }
                }),
                CompletableFuture.runAsync(() -> {
                    for (int i = 0; i < numItems; i++) {
                        channel1.write(i);
                        try {
                            assertEquals(i, run(channel2::read));
                        } catch (Exception ex) {
                            fail(ex);
                        }
                    }
                })
        ).get();
    }

    @ParameterizedTest
    @CsvSource({
            "1, 1",
            "1, 10",
            "10, 1",
            "10, 10"
    })
    @SuppressWarnings("unsafe")
    void manyProducerConsumerConcurrentReadWriteSuccess(int numReaders, int numWriters, Channel<Integer> channel) throws Exception {
        if (requiresSingleReader && numReaders > 1) {
            return;
        }

        if (requiresSingleWriter && numWriters > 1) {
            return;
        }

        int numItems = 10000;

        var readTotal = new LongAdder();
        var remainingWriters = new AtomicInteger(numWriters);
        var remainingItems = new AtomicInteger(numItems);

        var futures = new CompletableFuture[numWriters + numReaders];

        for (int i = 0; i < numReaders; i++) {
            futures[i] = CompletableFuture.runAsync(() -> {
                assertThrowsCause(ChannelClosedException.class, () -> {
                    while (true) {
                        readTotal.add(run(channel::read));
                    }
                });
            });
        }
        for (int i = 0; i < numWriters; i++) {
            futures[numReaders + i] = CompletableFuture.runAsync(() -> {
                while (true) {
                    int value = remainingItems.decrementAndGet();
                    if (value < 0) {
                        break;
                    }

                    runOk(channel.write(value + 1));
                }
                if (remainingWriters.decrementAndGet() == 0) {
                    channel.complete();
                }
            });
        }

        CompletableFuture.allOf(futures).get();
        run(channel::completion);

        assertEquals((numItems * (numItems + 1L)) / 2, readTotal.longValue());
    }

    @Test
    void pollDataAvailableSuccess(Channel<Integer> channel) {
        channel.write(42);
        var read = channel.poll();

        assertNotNull(read);
        assertEquals(42, read);
    }

    @Test
    void pollAfterCompleteReturnsNull(Channel<Integer> channel) {
        channel.complete();
        assertNull(channel.poll());
    }

    @Test
    void offerAfterCompleteReturnsFalse(Channel<Integer> channel) {
        channel.complete();
        assertFalse(channel.offer(42));
    }

    @Test
    void writeAfterCompleteThrowsException(Channel<Integer> channel) {
        channel.complete();
        assertThrowsCause(ChannelClosedException.class, exec(() -> channel.write(42)));
    }

    @Test
    void completeWithExceptionPropagatesToCompletion(Channel<Integer> channel) {
        var ex = new IllegalArgumentException();
        channel.complete(ex);
        assertEquals(ex, assertThrows(ExecutionException.class, exec(channel::read)).getCause());
    }

    @Test
    void completeWithExceptionPropagatesToExistingWriter() {
        var channel = createFullIntChannel();
        if (channel == null) return;

        var write = channel.write(42);
        var ex = new IllegalArgumentException();
        channel.complete(ex);
        assertEquals(ex, assertThrows(ExecutionException.class, exec(() -> write)).getCause());
    }

    @Test
    void completeWithExceptionPropagatesToNewWriter(Channel<Integer> channel) {
        var ex = new IllegalArgumentException();
        channel.complete(ex);
        assertEquals(ex, assertThrows(ExecutionException.class, exec(() -> channel.write(42))).getCause());
    }

    @Test
    @SuppressWarnings("unchecked")
    void manyWriteThenManyTryReadSuccess(Channel<Integer> channel) {
        if (requiresSingleReader || requiresSingleWriter) {
            return;
        }

        int numItems = 2000;

        var writers = new CompletableFuture[numItems];
        for (int i = 0; i < writers.length; i++) {
            writers[i] = channel.write(i).toCompletableFuture();
        }

        var readers = new CompletableFuture[numItems];
        for (int i = 0; i < readers.length; i++) {
            var result = channel.poll();
            assertNotNull(result);
            assertEquals(i, result);
        }

        assertTrue(
                Arrays.stream(writers)
                .peek(TestHelper::runOk)
                .allMatch(CompletableFuture::isDone)
        );
    }

    @Test
    void readThenWriteAsyncSucceeds(Channel<Integer> channel) throws Exception {
        var read = channel.read().toCompletableFuture();
        assertFalse(read.isDone());

        var write = channel.write(42).toCompletableFuture();
        assertTrue(write.isDone());

        assertEquals(42, read.get());
    }

    @Test
    @SuppressWarnings("unchecked")
    void readWriteManyConcurrentReadersSerializedWritersSuccess(Channel<Integer> channel) {
        if (requiresSingleReader) {
            return;
        }

        int items = 100;

        var readers = IntStream.range(0, items)
                .mapToObj(i -> channel.read())
                .toArray(CompletionStage[]::new);
        IntStream.range(0, items)
                .forEach(i -> runOk(channel.write(i)));

        int res = Arrays.stream(readers)
                .map(cs -> (CompletionStage<Integer>)cs)
                .mapToInt(TestHelper::runOk)
                .sum();
        assertEquals((items * (items - 1)) / 2, res);
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    void readOfferManyConcurrentReadersSerializedWritersSuccess(Channel<Integer> channel) {
        if (requiresSingleReader) {
            return;
        }

        var items = 100;

        var readers = IntStream.range(0, items)
                .mapToObj(i -> channel.read().toCompletableFuture())
                .collect(toList());
        var total = 0;
        for (int i = 0; i < items; i++) {
            assertTrue(channel.offer(i));
            var result = (int)runOk(readers.get(i));
            assertEquals(i, result);
            total += result;
        }

        assertEquals((items * (items - 1)) / 2, total);
    }

    @Test
    void readAlreadyCompletedThrows(Channel<Integer> channel) {
        channel.complete();
        assertThrowsCause(ChannelClosedException.class, exec(channel::read));
    }

    @Test
    void readSubsequentlyCompletedThrows(Channel<Integer> channel) {
        var read = channel.read().toCompletableFuture();
        assertFalse(read.isDone());
        channel.complete();
        assertThrowsCause(ChannelClosedException.class, exec(() -> read));
    }

    @Test
    void readAfterFaultedChannelThrows(Channel<Integer> channel) {
        var ex = new IllegalArgumentException();
        channel.complete(ex);
        assertTrue(channel.completion().toCompletableFuture().isCompletedExceptionally());

        assertEquals(ex, assertThrows(ExecutionException.class, exec(channel::read)).getCause());
    }

    @Test
    void readConsecutiveReadsSucceed(Channel<Integer> channel) {
        for (int i = 0; i < 5; i++) {
            var r = channel.read();
            runOk(channel.write(i));
            assertEquals(i, runOk(r));
        }
    }

    @Test
    void readFuture(Channel<Integer> channel) {
        var cs = channel.read();
        assertCompletableFutureDoesNotLeak(cs);
    }

    <T> void assertCompletableFutureDoesNotLeak(CompletionStage<T> cs) {
        if (!(cs instanceof CompletableFuture)) {
            return;
        }

        completableFutureThrowsOnNonCompletionStageMethodCalls((CompletableFuture<T>) cs);
        assertNotEquals(cs, cs.toCompletableFuture());
    }

    private <T> void completableFutureThrowsOnNonCompletionStageMethodCalls(CompletableFuture<T> cf) {
        assertThrows(UnsupportedOperationException.class, cf::get);
        assertThrows(UnsupportedOperationException.class, () -> cf.get(1, TimeUnit.MILLISECONDS));
        assertThrows(UnsupportedOperationException.class, () -> cf.getNow(null));
        assertThrows(UnsupportedOperationException.class, cf::join);
        assertThrows(UnsupportedOperationException.class, () -> cf.complete(null));
        assertThrows(UnsupportedOperationException.class, () -> cf.completeExceptionally(null));
        assertThrows(UnsupportedOperationException.class, () -> cf.cancel(true));
        assertThrows(UnsupportedOperationException.class, () -> cf.obtrudeValue(null));
        assertThrows(UnsupportedOperationException.class, () -> cf.obtrudeException(null));
        assertThrows(UnsupportedOperationException.class, cf::isDone);
        assertThrows(UnsupportedOperationException.class, cf::isCancelled);
        assertThrows(UnsupportedOperationException.class, cf::isCompletedExceptionally);
        assertThrows(UnsupportedOperationException.class, cf::getNumberOfDependents);
        assertThrows(UnsupportedOperationException.class, () -> cf.completeAsync(() -> null, ForkJoinPool.commonPool()));
        assertThrows(UnsupportedOperationException.class, () -> cf.completeAsync(() -> null));
        assertThrows(UnsupportedOperationException.class, () -> cf.orTimeout(1, TimeUnit.MILLISECONDS));
        assertThrows(UnsupportedOperationException.class, () -> cf.completeOnTimeout(null, 1, TimeUnit.MILLISECONDS));
    }

}
