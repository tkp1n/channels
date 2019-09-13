package com.ndportmann.channels;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

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

    @Test
    void writeFuture() {
        var channel = createFullIntChannel();
        if (channel == null) {
            return;
        }

        var cs = channel.write(1);
        assertCompletableFutureDoesNotLeak(cs);
    }

    @Nested
    class ChannelReaderTests {
        @Test
        void pollConsumerEmpty(Channel<Integer> channel) {
            var sum = new AtomicInteger();
            var count = channel.poll(sum::addAndGet);

            assertEquals(0, sum.get());
            assertEquals(0, count);
        }

        @Test
        void pollConsumerManyItems(Channel<Integer> channel) throws Exception {
            int numItems = 100;
            CompletableFuture[] cfs = new CompletableFuture[numItems];
            for (int i = 0; i < cfs.length; i++) {
                cfs[i] = channel.write(i).toCompletableFuture();
            }

            var sum = new AtomicInteger();
            var count = channel.poll(sum::addAndGet);

            CompletableFuture.allOf(cfs).get();

            assertEquals((numItems - 1) * numItems / 2., sum.get());
            assertEquals(numItems, count);
        }

        @Test
        void manyPollConsumerManyItems(Channel<Integer> channel) throws Exception {
            int numItems = 10_000;
            CompletableFuture[] writes = new CompletableFuture[numItems];
            for (int i = 0; i < writes.length; i++) {
                writes[i] = channel.write(i).toCompletableFuture();
            }

            var sum = new AtomicInteger();
            var count = new AtomicInteger();
            CompletableFuture.allOf(
                CompletableFuture.runAsync(() -> count.addAndGet(channel.poll(sum::addAndGet))),
                CompletableFuture.runAsync(() -> count.addAndGet(channel.poll(sum::addAndGet))),
                CompletableFuture.runAsync(() -> count.addAndGet(channel.poll(sum::addAndGet)))
            ).get();

            CompletableFuture.allOf(writes).get();

            assertEquals((numItems - 1) * numItems / 2., sum.get());
            assertEquals(numItems, count.get());
        }

        @Test
        void pollConsumerAfterCompleteReturnsZero(Channel<Integer> channel) {
            channel.complete();

            var sum = new AtomicInteger();
            assertEquals(0, channel.poll(sum::addAndGet));
            assertEquals(0, sum.get());
        }

        @Test
        void pollAllArrayEmpty(Channel<Integer> channel) {
            var items = new Integer[10];
            var count = channel.pollAll(items);

            assertEquals(0, count);
            for (int i = 0; i < items.length; i++) {
                assertNull(items[i]);
            }
        }

        @Test
        void pollAllArrayManyItems(Channel<Integer> channel) throws Exception {
            int numItems = 100;
            CompletableFuture[] cfs = new CompletableFuture[numItems];
            for (int i = 0; i < cfs.length; i++) {
                cfs[i] = channel.write(i).toCompletableFuture();
            }

            var items = new Integer[numItems];
            var count = channel.pollAll(items);

            CompletableFuture.allOf(cfs).get();

            for (int i = 0; i < numItems; i++) {
                assertEquals(i, items[i]);
            }
            assertEquals(numItems, count);
        }

        @Test
        void pollAllArrayManyItemsArrayTooSmall(Channel<Integer> channel) throws Exception {
            int numItems = 100;
            CompletableFuture[] cfs = new CompletableFuture[numItems];
            for (int i = 0; i < cfs.length; i++) {
                cfs[i] = channel.write(i).toCompletableFuture();
            }

            var items = new Integer[numItems - 10];
            var count = channel.pollAll(items);

            CompletableFuture.allOf(Arrays.copyOf(cfs, count)).get();

            for (int i = 0; i < numItems - 10; i++) {
                assertEquals(i, items[i]);
            }
            assertEquals(numItems - 10, count);
        }

        @Test
        void manyPollAllArrayManyItems(Channel<Integer> channel) throws Exception {
            int numItems = 10_000;
            CompletableFuture[] writes = new CompletableFuture[numItems];
            for (int i = 0; i < writes.length; i++) {
                writes[i] = channel.write(i).toCompletableFuture();
            }

            var arr1 = new Integer[numItems];
            var arr2 = new Integer[numItems];
            var arr3 = new Integer[numItems];
            var count = new AtomicInteger();
            CompletableFuture.allOf(
                    CompletableFuture.runAsync(() -> count.addAndGet(channel.pollAll(arr1))),
                    CompletableFuture.runAsync(() -> count.addAndGet(channel.pollAll(arr2))),
                    CompletableFuture.runAsync(() -> count.addAndGet(channel.pollAll(arr3)))
            ).get();

            CompletableFuture.allOf(writes).get();

            assertTrue(isSorted(arr1));
            assertTrue(isSorted(arr2));
            assertTrue(isSorted(arr3));

            var all = Stream.of(
                    Arrays.stream(arr1).filter(Objects::nonNull),
                    Arrays.stream(arr2).filter(Objects::nonNull),
                    Arrays.stream(arr3).filter(Objects::nonNull)
            ).reduce(Stream::concat).orElseGet(Stream::empty)
            .mapToInt(i -> i)
            .toArray();

            Arrays.sort(all);

            assertEquals(numItems, all.length);
            for (int i = 0; i < all.length; i++) {
                assertEquals(i, all[i]);
            }

            assertEquals(numItems, count.get());
        }

        @Test
        void pollAllArrayAfterCompleteReturnsZero(Channel<Integer> channel) {
            channel.complete();

            var items = new Integer[1];
            assertEquals(0, channel.pollAll(items));
            assertNull(items[0]);
        }

        @Test
        void pollAllCollectionEmpty(Channel<Integer> channel) {
            var items = new ArrayList<Integer>();
            var count = channel.pollAll(items);

            assertEquals(0, count);
            assertEquals(0, items.size());
        }

        @Test
        void pollAllCollectionManyItems(Channel<Integer> channel) throws Exception {
            int numItems = 100;
            CompletableFuture[] cfs = new CompletableFuture[numItems];
            for (int i = 0; i < cfs.length; i++) {
                cfs[i] = channel.write(i).toCompletableFuture();
            }

            var items = new ArrayList<Integer>();
            var count = channel.pollAll(items);

            CompletableFuture.allOf(cfs).get();

            for (int i = 0; i < numItems; i++) {
                assertEquals(i, items.get(i));
            }
            assertEquals(numItems, count);
        }

        @Test
        void manyPollAllCollectionManyItems(Channel<Integer> channel) throws Exception {
            int numItems = 10_000;
            CompletableFuture[] writes = new CompletableFuture[numItems];
            for (int i = 0; i < writes.length; i++) {
                writes[i] = channel.write(i).toCompletableFuture();
            }

            var arr1 = new ArrayList<Integer>();
            var arr2 = new ArrayList<Integer>();
            var arr3 = new ArrayList<Integer>();
            var count = new AtomicInteger();
            CompletableFuture.allOf(
                    CompletableFuture.runAsync(() -> count.addAndGet(channel.pollAll(arr1))),
                    CompletableFuture.runAsync(() -> count.addAndGet(channel.pollAll(arr2))),
                    CompletableFuture.runAsync(() -> count.addAndGet(channel.pollAll(arr3)))
            ).get();

            CompletableFuture.allOf(writes).get();

            assertTrue(isSorted(arr1));
            assertTrue(isSorted(arr2));
            assertTrue(isSorted(arr3));

            arr1.addAll(arr2);
            arr1.addAll(arr3);

            arr1.sort(Comparator.naturalOrder());

            assertEquals(numItems, arr1.size());
            for (int i = 0; i < numItems; i++) {
                assertEquals(i, arr1.get(i));
            }

            assertEquals(numItems, count.get());
        }

        @Test
        void pollAllCollectionAfterCompleteReturnsZero(Channel<Integer> channel) {
            channel.complete();;

            var items = new ArrayList<Integer>();
            assertEquals(0, channel.pollAll(items));
            assertEquals(0, items.size());
        }

        @Test
        void pollAllEmpty(Channel<Integer> channel) {
            var items = channel.pollAll();

            assertNotNull(items);
            assertEquals(0, items.size());
        }

        @Test
        void pollAllManyItems(Channel<Integer> channel) throws Exception {
            int numItems = 100;
            CompletableFuture[] cfs = new CompletableFuture[numItems];
            for (int i = 0; i < cfs.length; i++) {
                cfs[i] = channel.write(i).toCompletableFuture();
            }

            var items = channel.pollAll();

            CompletableFuture.allOf(cfs).get();

            assertEquals(numItems, items.size());
            for (int i = 0; i < numItems; i++) {
                assertEquals(i, items.get(i));
            }
        }

        @Test
        void manyPollAllManyItems(Channel<Integer> channel) throws Exception {
            int numItems = 10_000;
            CompletableFuture[] writes = new CompletableFuture[numItems];
            for (int i = 0; i < writes.length; i++) {
                writes[i] = channel.write(i).toCompletableFuture();
            }

            Executor executor = Executors.newFixedThreadPool(4);
            CompletionService<List<Integer>> completionService = new ExecutorCompletionService<>(executor);

            completionService.submit(() -> channel.pollAll());
            completionService.submit(() -> channel.pollAll());
            completionService.submit(() -> channel.pollAll());

            var arr1 = completionService.take().get();
            var arr2 = completionService.take().get();
            var arr3 = completionService.take().get();

            CompletableFuture.allOf(writes).get();

            assertTrue(isSorted(arr1));
            assertTrue(isSorted(arr2));
            assertTrue(isSorted(arr3));

            arr1.addAll(arr2);
            arr1.addAll(arr3);

            arr1.sort(Comparator.naturalOrder());

            assertEquals(numItems, arr1.size());
            for (int i = 0; i < numItems; i++) {
                assertEquals(i, arr1.get(i));
            }
        }

        @Test
        void pollAllAfterCompleteReturnsZero(Channel<Integer> channel) {
            channel.complete();

            assertEquals(0, channel.pollAll().size());
        }

        @Test
        void readAllConsumerEmpty(Channel<Integer> channel) {
            var sum = new AtomicInteger();
            var completion = channel.readAll(sum::addAndGet).toCompletableFuture();

            channel.complete();
            runOk(completion);

            assertTrue(completion.isDone());
            assertEquals(0, sum.get());
        }

        @Test
        void readAllConsumerEmptyExceptionalCompletion(Channel<Integer> channel) {
            var sum = new AtomicInteger();
            var completion = channel.readAll(sum::addAndGet).toCompletableFuture();

            var ex = new RuntimeException();

            channel.complete(ex);
            assertThrowsCause(ex, completion::get);

            assertTrue(completion.isDone());
            assertEquals(0, sum.get());
        }

        @Test
        void readAllConsumerManyItems(Channel<Integer> channel) throws Exception {
            int numItems = 1000;
            int half = numItems / 2;

            var writes = new CompletableFuture[numItems];
            for (int i = 0; i < half; i++) {
                writes[i] = channel.write(i).toCompletableFuture();
            }

            var items = new ArrayList<Integer>();
            var completion = channel.readAll(items::add).toCompletableFuture();

            for (int i = half; i < numItems; i++) {
                writes[i] = channel.write(i).toCompletableFuture();
            }

            CompletableFuture.allOf(writes).get();

            channel.complete();
            assertNull(completion.get());

            assertEquals(numItems, items.size());
            for (int i = 0; i < numItems; i++) {
                assertEquals(i, items.get(i));
            }
        }

        @Test
        void readAllConsumerManyItemsExceptionalCompletion(Channel<Integer> channel) throws Exception {
            int numItems = 1000;
            int half = numItems / 2;

            var writes = new CompletableFuture[numItems];
            for (int i = 0; i < half; i++) {
                writes[i] = channel.write(i).toCompletableFuture();
            }

            var items = new ArrayList<Integer>();
            var completion = channel.readAll(items::add).toCompletableFuture();

            for (int i = half; i < numItems; i++) {
                writes[i] = channel.write(i).toCompletableFuture();
            }

            CompletableFuture.allOf(writes).get();

            var ex = new RuntimeException();

            channel.complete(ex);
            assertThrowsCause(ex, completion::get);

            assertEquals(numItems, items.size());
            for (int i = 0; i < numItems; i++) {
                assertEquals(i, items.get(i));
            }
        }

        @Test
        void readAllArrayEmpty(Channel<Integer> channel) {
            var res = new Integer[1];
            var completion = channel.readAll(res).toCompletableFuture();

            channel.complete();
            assertEquals(0, runOk(completion));

            assertNull(res[0]);
        }

        @Test
        void readAllArrayEmptyExceptionalCompletion(Channel<Integer> channel) {
            var res = new Integer[1];
            var completion = channel.readAll(res).toCompletableFuture();

            var ex = new RuntimeException();

            channel.complete(ex);
            assertThrowsCause(ex, completion::get);

            assertNull(res[0]);
        }

        @ParameterizedTest
        @CsvSource({
                "1000, 1001",
                "1000, 1000",
                "1000, 999",
        })
        void readAllArrayConsumerManyItems(int numItems, int capacity, Channel<Integer> channel) throws Exception {
            int half = numItems / 2;

            var writes = new CompletableFuture[numItems];
            for (int i = 0; i < half; i++) {
                writes[i] = channel.write(i).toCompletableFuture();
            }

            var items = new Integer[capacity];
            var completion = channel.readAll(items).toCompletableFuture();

            for (int i = half; i < numItems; i++) {
                writes[i] = channel.write(i).toCompletableFuture();
            }

            CompletableFuture.allOf(writes).get();

            channel.complete();
            int written = Math.min(numItems, capacity);
            assertEquals(written, completion.get());

            int i;
            for (i = 0; i < written; i++) {
                assertEquals(i, items[i]);
            }
            for (; i < capacity; i++) {
                assertNull(items[i]);
            }
        }

        @ParameterizedTest
        @CsvSource({
                "1000, 1001",
                "1000, 1000",
                "1000, 999",
        })
        void readAllArrayConsumerManyItemsExceptionalCompletion(int numItems, int capacity, Channel<Integer> channel) throws Exception {
            int half = numItems / 2;

            var writes = new CompletableFuture[numItems];
            for (int i = 0; i < half; i++) {
                writes[i] = channel.write(i).toCompletableFuture();
            }

            var items = new Integer[capacity];
            var completion = channel.readAll(items).toCompletableFuture();

            for (int i = half; i < numItems; i++) {
                writes[i] = channel.write(i).toCompletableFuture();
            }

            CompletableFuture.allOf(writes).get();

            var ex = new RuntimeException();

            channel.complete(ex);
            int written = Math.min(numItems, capacity);
            if (capacity > numItems) {
                assertThrowsCause(ex, completion::get);
            } else {
                assertEquals(written, completion.get());
            }

            int i;
            for (i = 0; i < written; i++) {
                assertEquals(i, items[i]);
            }
            for (; i < capacity; i++) {
                assertNull(items[i]);
            }
        }

        @Test
        void readAllCollectionEmpty(Channel<Integer> channel) {
            var items = new ArrayList<Integer>();
            var completion = channel.readAll(items).toCompletableFuture();

            channel.complete();
            runOk(completion);

            assertTrue(completion.isDone());
            assertEquals(0, items.size());
        }

        @Test
        void readAllCollectionEmptyExceptionalCompletion(Channel<Integer> channel) {
            var items = new ArrayList<Integer>();
            var completion = channel.readAll(items).toCompletableFuture();

            var ex = new RuntimeException();

            channel.complete(ex);
            assertThrowsCause(ex, completion::get);

            assertTrue(completion.isDone());
            assertEquals(0, items.size());
        }

        @Test
        void readAllCollectionManyItems(Channel<Integer> channel) throws Exception {
            int numItems = 1000;
            int half = numItems / 2;

            var writes = new CompletableFuture[numItems];
            for (int i = 0; i < half; i++) {
                writes[i] = channel.write(i).toCompletableFuture();
            }

            var items = new ArrayList<Integer>();
            var completion = channel.readAll(items).toCompletableFuture();

            for (int i = half; i < numItems; i++) {
                writes[i] = channel.write(i).toCompletableFuture();
            }

            CompletableFuture.allOf(writes).get();

            channel.complete();
            assertNull(completion.get());

            assertEquals(numItems, items.size());
            for (int i = 0; i < numItems; i++) {
                assertEquals(i, items.get(i));
            }
        }

        @Test
        void readAllCollectionManyItemsExceptionalCompletion(Channel<Integer> channel) throws Exception {
            int numItems = 1000;
            int half = numItems / 2;

            var writes = new CompletableFuture[numItems];
            for (int i = 0; i < half; i++) {
                writes[i] = channel.write(i).toCompletableFuture();
            }

            var items = new ArrayList<Integer>();
            var completion = channel.readAll(items).toCompletableFuture();

            for (int i = half; i < numItems; i++) {
                writes[i] = channel.write(i).toCompletableFuture();
            }

            CompletableFuture.allOf(writes).get();

            var ex = new RuntimeException();

            channel.complete(ex);
            assertThrowsCause(ex, completion::get);

            assertEquals(numItems, items.size());
            for (int i = 0; i < numItems; i++) {
                assertEquals(i, items.get(i));
            }
        }
    }

    private static <T> void assertCompletableFutureDoesNotLeak(CompletionStage<T> cs) {
        if (!(cs instanceof CompletableFuture)) {
            return;
        }

        completableFutureThrowsOnNonCompletionStageMethodCalls((CompletableFuture<T>) cs);
        assertNotEquals(cs, cs.toCompletableFuture());
    }

    private static <T> void completableFutureThrowsOnNonCompletionStageMethodCalls(CompletableFuture<T> cf) {
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

    private static boolean isSorted(Integer[] arr) {
        Integer last = Integer.MIN_VALUE;
        for (int i = 0; i < arr.length; i++) {
            if (arr[i] == null) {
                return true;
            }
            if (arr[i] <= last) {
                if (arr[i] < last) {
                    return false;
                }
                if (last != Integer.MIN_VALUE) {
                    return false;
                }
            }
            last = arr[i];
        }
        return true;
    }

    private static boolean isSorted(Collection<Integer> arr) {
        Integer last = Integer.MIN_VALUE;
        for (Integer integer : arr) {
            if (integer == null) {
                return true;
            }
            if (integer <= last) {
                if (integer < last) {
                    return false;
                }
                if (last != Integer.MIN_VALUE) {
                    return false;
                }
            }
            last = integer;
        }
        return true;
    }
}
