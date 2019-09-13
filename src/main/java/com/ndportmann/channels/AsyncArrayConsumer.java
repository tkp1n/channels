package com.ndportmann.channels;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

final class AsyncArrayConsumer<T> {
    private final ChannelReader<T> reader;
    private final CompletableFuture<Void> completion;
    private final T[] array;
    private int index = 0;

    AsyncArrayConsumer(ChannelReader<T> reader, T[] array) {
        this.reader = reader;
        this.completion = reader.completion().toCompletableFuture();
        this.array = array;
    }

    static <T> CompletionStage<Integer> read(ChannelReader<T> reader, T[] array) {
        return new AsyncArrayConsumer<>(reader, array).read();
    }

    final CompletionStage<Integer> read() {
        final var arr = array;
        final var r = reader;
        T curr;
        while (index < arr.length && (curr = r.poll()) != null) {
            arr[index++] = curr;
        }
        if (index >= arr.length) {
            return CompletableFuture.completedFuture(index);
        } else if (completion.isDone()) {
            return completion.thenApply(v -> index);
        }

        return handle(r.read().thenComposeAsync(this::consumeAndReadNext));
    }

    private CompletionStage<Integer> consumeAndReadNext(T item) {
        final var arr = array;
        assert index < arr.length; // async read path is only taken, if there is still space for the item being read

        arr[index++] = item;

        return read();
    }

    private CompletionStage<Integer> handle(CompletionStage<Integer> target) {
        return target.handle((result, error) -> {
            if (error != null) {
                if (error.getCause() instanceof ChannelClosedException) {
                    // Operation was interrupted by completion of the channel -> return nof written items
                    return index;
                }

                // Operation was interrupted by an actual error -> throw
                AsyncConsumerHelper.sneakyThrow(error);
            }

            // Operation yielded normal result -> pass through
            return result;
        });
    }
}
