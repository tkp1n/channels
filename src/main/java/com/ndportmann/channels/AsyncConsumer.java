package com.ndportmann.channels;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

final class AsyncConsumer<T> {
    private final ChannelReader<T> reader;
    private final CompletableFuture<Void> completion;
    private final Consumer<T> consumer;

    private AsyncConsumer(ChannelReader<T> reader, Consumer<T> consumer) {
        this.reader = reader;
        this.completion = reader.completion().toCompletableFuture();
        this.consumer = consumer;
    }

    static <T> CompletionStage<Void> read(ChannelReader<T> reader, Consumer<T> consumer) {
        return new AsyncConsumer<T>(reader, consumer).read();
    }

    final CompletionStage<Void> read() {
        reader.poll(consumer);
        if (completion.isDone()) {
            return completion;
        }

        return handle(reader.read().thenComposeAsync(r -> {
            consumer.accept(r);
            return read();
        }));
    }

    private static CompletionStage<Void> handle(CompletionStage<Void> target) {
        return target.handle((result, error) -> {
            if (error != null) {
                if (error.getCause() instanceof ChannelClosedException) {
                    // Operation was interrupted by completion of the channel -> ignore
                    return null;
                }

                // Operation was interrupted by an actual error -> throw
                AsyncConsumerHelper.sneakyThrow(error);
            }

            // Operation yielded normal result -> pass through
            return result;
        });
    }
}
