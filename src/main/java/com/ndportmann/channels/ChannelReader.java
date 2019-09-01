package com.ndportmann.channels;

import java.util.concurrent.CompletionStage;

/**
 * The readable half of a {@link DualChannel}
 *
 * @param <T> The items being read
 */
public interface ChannelReader<T> {
    /**
     * Attempts to read an item from the channel.
     *
     * @return The read item, or null if no item could be read.
     */
    T poll();

    /**
     * Asynchronously reads an item from the channel.
     *
     * @return A {@link CompletionStage} that represents the asynchronous read operation.
     */
    CompletionStage<T> read();

    /**
     * Gets a {@link CompletionStage} that completes when no more data will ever be available to be read from this channel.
     * @return A {@link CompletionStage} that completes when no more data will ever be available to be read from this channel.
     */
    CompletionStage<Void> completion();
}
