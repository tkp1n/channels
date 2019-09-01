package com.ndportmann.channels;

import java.util.concurrent.CompletionStage;

/**
 * The writable half of a {@link DualChannel}
 *
 * @param <T> The items being written
 */
public interface ChannelWriter<T> {
    /**
     * Attempts to write the specified item to the channel.
     *
     * @param item The item to write.
     * @return true if the item was written; otherwise, false.
     */
    boolean offer(T item);

    /**
     * Asynchronously writes an item to the channel.
     * The returned {@link CompletionStage} may complete exceptionally if the channel is closed
     * prior to successful completion.
     *
     * @param item The value to write to the channel.
     * @return A {@link CompletionStage} that represents the asynchronous write operation.
     */
    CompletionStage<Void> write(T item);

    /**
     * Mark the channel as being complete, meaning no more items will be written to it.
     *
     * @throws IllegalStateException The channel has already been marked as complete.
     */
    default void complete() {
        complete(null);
    }

    /**
     * Mark the channel as being complete, meaning no more items will be written to it.
     *
     * @param error Optional Exception indicating a failure that's causing the channel to complete.
     * @throws IllegalStateException The channel has already been marked as complete.
     */
    default void complete(Throwable error) {
        if (!tryComplete(error)) {
            throw new IllegalStateException("Channel already completed");
        }
    }

    /**
     * Attempts to mark the channel as being completed, meaning no more data will be written to it.
     * @return true if this operation successfully completes the channel; otherwise,
     * false if the channel could not be marked for completion, for example due to having already been marked as such,
     * or due to not supporting completion.
     */
    default boolean tryComplete() {
        return tryComplete(null);
    }

    /**
     * Attempts to mark the channel as being completed, meaning no more data will be written to it.
     *
     * @param error An Exception indicating the failure causing no more data to be written, or null for success.
     * @return true if this operation successfully completes the channel; otherwise,
     * false if the channel could not be marked for completion, for example due to having already been marked as such,
     * or due to not supporting completion.
     */
    boolean tryComplete(Throwable error);
}
