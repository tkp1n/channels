package com.ndportmann.channels;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

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

    /**
     * Attempts to read items from the channel and supply it to the provided {@link Consumer} until
     * {@link #poll()} returns false.
     *
     * @param consumer The {@link Consumer} to accept the polled items.
     * @return The number of items added to the provided {@link Collection}.
     */
    default int poll(Consumer<T> consumer) {
        Objects.requireNonNull(consumer);

        int i = 0;
        T item;
        while ((item = poll()) != null) {
            consumer.accept(item);
            i++;
        }
        return i;
    }

    /**
     * Attempts to read items from the channel into the provided array until it is full
     * or {@link #poll()} returns false.
     *
     * @param items The array to contain the polled items.
     * @return The number of items written to the provided array.
     */
    default int pollAll(T[] items) {
        Objects.requireNonNull(items);

        int i;
        T item;
        for (i = 0; i < items.length && (item = poll()) != null; i++) {
            items[i] = item;
        }

        return i;
    }

    /**
     * Attempts to read items from the channel into the provided {@link Collection} until
     * {@link #poll()} returns false.
     *
     * @param items The {@link Collection} to contain the polled items.
     * @return The number of items added to the provided {@link Collection}.
     */
    default int pollAll(Collection<T> items) {
        Objects.requireNonNull(items);
        return poll(items::add);
    }

    /**
     * Attempts to read items from the channel until {@link #poll()} returns false.
     *
     * @return The items polled from the channel.
     */
    default List<T> pollAll() {
        var items = new ArrayList<T>();
        pollAll(items);
        return items;
    }

    /**
     * Asynchronously consumes all items written to the channel and supplies it to the provided {@link Consumer},
     * until the channel is closed.
     * Once the channel is closed, this method returns the same result as {@link ChannelReader#completion()}.
     *
     * @param consumer The {@link Consumer} to accept the polled items.
     * @return The completion of the {@link ChannelReader}
     */
    default CompletionStage<Void> readAll(Consumer<T> consumer) {
        Objects.requireNonNull(consumer);
        return AsyncConsumer.read(this, consumer);
    }

    /**
     * Asynchronously attempts to read items from the channel into the provided array until it is full
     * or the channel is closed.
     *
     * @param items The array to contain the read items.
     * @return The number of items written to the array. If the channel is completed exceptionally,
     * this method returns the same result as {@link ChannelReader#completion()}.
     */
    default CompletionStage<Integer> readAll(T[] items) {
        Objects.requireNonNull(items);
        return AsyncArrayConsumer.read(this, items);
    }

    /**
     * Asynchronously attempts to read items from the channel into the provided {@link Collection}, until
     * the channel is closed.
     * This method returns the same result as {@link ChannelReader#completion()}.
     *
     * @param items The {@link Collection} to contain the read items.
     * @return The completion of the {@link ChannelReader}
     */
    default CompletionStage<Void> readAll(Collection<T> items) {
        return AsyncConsumer.read(this, items::add);
    }
}
