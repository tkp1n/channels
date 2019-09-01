package com.ndportmann.channels;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

abstract class UnboundedChannelTests extends ChannelTestBase {
    @Test
    void completeBeforeEmptyNoWaitersTriggersCompletion(Channel<Integer> channel) {
        assertTrue(channel.offer(42));
        channel.complete();
        var completion = channel.completion().toCompletableFuture();
        assertFalse(completion.isDone());
        assertEquals(42, runOk(channel.read()));
        runOk(completion);
    }

    @Test
    void offerPollMany(Channel<Integer> channel) {
        int numItems = 100000;
        for (int i = 0; i < numItems; i++) {
            assertTrue(channel.offer(i));
        }
        for (int i = 0; i < numItems; i++) {
            assertEquals(i, channel.poll());
        }
    }

    @Test
    void offerPollOneAtATime(Channel<Integer> channel) {
        int numItems = 100000;
        for (int i = 0; i < numItems; i++) {
            assertTrue(channel.offer(i));
            assertEquals(i, channel.poll());
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1})
    void writeManyThenCompleteSuccessfullyReadAll(int readMode, Channel<Integer> channel) {
        for (int i = 0; i < 10; i++) {
            assertTrue(channel.offer(i));
        }

        channel.complete();
        var completion = channel.completion().toCompletableFuture();

        for (int i = 0; i < 10; i++) {
            assertFalse(completion.isDone());
            switch (readMode) {
                case 0:
                    assertEquals(i, channel.poll());
                    break;
                case 1:
                    assertEquals(i, runOk(channel.read()));
                    break;
            }
        }

        runOk(completion);
    }

    @Override
    protected <T> Channel<T> createChannel() {
        var builder = Channel.createUnbounded();
        if (allowSynchronousContinuations) {
            builder = builder.allowSynchronousContinuations();
        }
        return builder.build();
    }

    @Override
    protected <T> Channel<T> createFullChannel(Supplier<T> values) {
        return null;
    }
}
