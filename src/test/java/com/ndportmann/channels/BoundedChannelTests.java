package com.ndportmann.channels;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;


import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

abstract class BoundedChannelTests extends ChannelTestBase {
    @ParameterizedTest
    @ValueSource(ints = {1, 10, 10000})
    void offerPollManyWait(int capacity) {
        var channel = Channel.createBounded(capacity)
                        .build();

        for (int i = 0; i < capacity; i++) {
            assertTrue(channel.writer().offer(i));
        }

        assertFalse(channel.writer().offer(capacity));

        for (int i = 0; i < capacity; i++) {
            var result = channel.reader().poll();
            assertNotNull(result);
            assertEquals(i, result);
        }

        assertNull(channel.reader().poll());
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 10, 10000})
    void offerPollManyDropOldest(int capacity) {
        var channel = Channel.createBounded(capacity)
                .fullMode(BoundedChannelFullMode.DROP_OLDEST)
                .build();

        for (int i = 0; i < capacity * 2; i++) {
            assertTrue(channel.writer().offer(i));
        }

        for (int i = capacity; i < capacity * 2; i++) {
            var result = channel.reader().poll();
            assertNotNull(result);
            assertEquals(i, result);
        }

        assertNull(channel.reader().poll());
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 10, 10000})
    void writePollManyDropOldest(int capacity) throws Exception {
        var channel = Channel.createBounded(capacity)
                .fullMode(BoundedChannelFullMode.DROP_OLDEST)
                .build();

        for (int i = 0; i < capacity * 2; i++) {
            channel.writer().write(i).toCompletableFuture().get();
        }

        for (int i = capacity; i < capacity * 2; i++) {
            var result = channel.reader().poll();
            assertNotNull(result);
            assertEquals(i, result);
        }

        assertNull(channel.reader().poll());
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 10, 10000})
    void offerPollManyDropNewest(int capacity) {
        var channel = Channel.createBounded(capacity)
                .fullMode(BoundedChannelFullMode.DROP_NEWEST)
                .<Integer>build();

        for (int i = 0; i < capacity * 2; i++) {
            assertTrue(channel.writer().offer(i));
        }

        Integer result;
        for (int i = 0; i < capacity - 1; i++) {
            result = channel.reader().poll();
            assertNotNull(result);
            assertEquals(i, result);
        }

        result = channel.reader().poll();
        assertNotNull(result);
        assertEquals(capacity * 2 - 1, result);

        assertNull(channel.reader().poll());
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 10, 10000})
    void writePollManyDropNewest(int capacity) throws Exception {
        var channel = Channel.createBounded(capacity)
                .fullMode(BoundedChannelFullMode.DROP_NEWEST)
                .<Integer>build();

        for (int i = 0; i < capacity * 2; i++) {
            channel.writer().write(i).toCompletableFuture().get();
        }

        Integer result;
        for (int i = 0; i < capacity - 1; i++) {
            result = channel.reader().poll();
            assertNotNull(result);
            assertEquals(i, result);
        }

        result = channel.reader().poll();
        assertNotNull(result);
        assertEquals(capacity * 2 - 1, result);

        assertNull(channel.reader().poll());
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 10, 10000})
    void offerPollManyDropWrite(int capacity) {
        var channel = Channel.createBounded(capacity)
                .fullMode(BoundedChannelFullMode.DROP_WRITE)
                .build();

        for (int i = 0; i < capacity * 2; i++) {
            assertTrue(channel.writer().offer(i));
        }

        for (int i = 0; i < capacity; i++) {
            var result = channel.reader().poll();
            assertNotNull(result);
            assertEquals(i, result);
        }

        assertNull(channel.reader().poll());
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 10, 10000})
    void writePollManyDropWrite(int capacity) throws Exception {
        var channel = Channel.createBounded(capacity)
                .fullMode(BoundedChannelFullMode.DROP_WRITE)
                .build();

        for (int i = 0; i < capacity * 2; i++) {
            channel.writer().write(i).toCompletableFuture().get();
        }

        for (int i = 0; i < capacity; i++) {
            var result = channel.reader().poll();
            assertNotNull(result);
            assertEquals(i, result);
        }

        assertNull(channel.reader().poll());
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 10, 10000})
    void singleProducerConsumerConcurrentReadWrite(int capacity) throws Exception {
        var channel = Channel.createBounded(capacity)
                .fullMode(BoundedChannelFullMode.WAIT)
                .build();

        int numItems = 10000;
        var writes = CompletableFuture.allOf(
                IntStream.range(0, numItems)
                    .mapToObj(i -> channel.writer().write(i).toCompletableFuture())
                    .toArray(CompletableFuture[]::new)
        );
        var reads = CompletableFuture.allOf(
                IntStream.range(0, numItems)
                        .mapToObj(i -> channel.reader().read()
                                .thenAccept(res -> assertEquals(i, res))
                                .toCompletableFuture())
                        .toArray(CompletableFuture[]::new)
        );

        CompletableFuture.allOf(writes, reads).get();
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 10, 10000})
    void manyProducerConsumerConcurrentReadWrite(int capacity) throws Exception {
        var channel = Channel.createBounded(capacity)
                .fullMode(BoundedChannelFullMode.WAIT)
                .<Integer>build();

        int numWriters = 10;
        int numReaders = 10;
        int numItems = 10000;

        var readTotal = new LongAdder();
        var remainingWriters = new AtomicInteger(numWriters);
        var remainingItems = new AtomicInteger(numItems);

        var futures = new CompletableFuture[numWriters + numReaders];

        for (int i = 0; i < numReaders; i++) {
            futures[i] = CompletableFuture.runAsync(() -> {
                try {
                    while (true) {
                        readTotal.add(channel.reader().read().toCompletableFuture().get());
                    }
                } catch (InterruptedException | ExecutionException e) {
                    if(!(e.getCause() instanceof  ChannelClosedException)) {
                        fail(e);
                    }
                }
            });
        }
        for (int i = 0; i < numWriters; i++) {
            futures[numReaders + i] = CompletableFuture.runAsync(() -> {
                while (true) {
                    int value = remainingItems.decrementAndGet();
                    if (value < 0) {
                        break;
                    }
                    try {
                        channel.writer().write(value + 1).toCompletableFuture().get();
                    } catch (InterruptedException | ExecutionException e) {
                        fail(e);
                    }
                }
                if (remainingWriters.decrementAndGet() == 0) {
                    channel.writer().complete();
                }
            });
        }

        CompletableFuture.allOf(futures).get();
        channel.reader().completion().toCompletableFuture().get();

        assertEquals((numItems * (numItems + 1L)) / 2, readTotal.longValue());
    }

    @Test
    void writeBeforeRead() {
        var channel = Channel.createBounded(1)
                .allowSynchronousContinuations()
                .<Integer>build();

        assertTrue(channel.writer().offer(1));

        var fut = channel.writer().write(2).toCompletableFuture();
        assertFalse(fut.isDone());

        assertEquals(1, channel.reader().poll());
        assertTrue(fut.isDone()); // write completed during poll, as space became available

        assertEquals(2, channel.reader().poll());
    }

    @Test
    void pollCompletes() {
        var channel = Channel.createBounded(1)
                .allowSynchronousContinuations()
                .<Integer>build();

        assertTrue(channel.offer(1));

        var completion = channel.reader().completion().toCompletableFuture();
        assertFalse(completion.isDone());

        channel.writer().complete(new RuntimeException());
        assertFalse(completion.isDone());

        assertEquals(1, channel.reader().poll());
        assertTrue(completion.isDone());
    }

    @Override
    protected <T> Channel<T> createChannel() {
        var builder = Channel.createBounded(1);
        if (allowSynchronousContinuations) {
            builder = builder.allowSynchronousContinuations();
        }
        return builder.build();
    }

    @Override
    protected <T> Channel<T> createFullChannel(Supplier<T> clazz) {
        var channel = this.<T>createChannel();
        try {
            channel.writer().offer(clazz.get());
        } catch (Exception e) {
            fail(e);
        }
        return channel;
    }
}