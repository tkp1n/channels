package com.ndportmann.channels;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static java.util.function.Function.identity;

abstract class MinimalFuture<T> extends CompletableFuture<T> {
    @Override public T get() { throw new UnsupportedOperationException(); }
    @Override public T get(long timeout, TimeUnit unit) { throw new UnsupportedOperationException(); }
    @Override public T getNow(T valueIfAbsent) { throw new UnsupportedOperationException(); }
    @Override public T join() { throw new UnsupportedOperationException(); }
    @Override public boolean complete(T value) { throw new UnsupportedOperationException(); }
    @Override public boolean completeExceptionally(Throwable ex) { throw new UnsupportedOperationException(); }
    @Override public boolean cancel(boolean mayInterruptIfRunning) { throw new UnsupportedOperationException(); }
    @Override public void obtrudeValue(T value) { throw new UnsupportedOperationException(); }
    @Override public void obtrudeException(Throwable ex) { throw new UnsupportedOperationException(); }
    @Override public boolean isDone() { throw new UnsupportedOperationException(); }
    @Override public boolean isCancelled() { throw new UnsupportedOperationException(); }
    @Override public boolean isCompletedExceptionally() { throw new UnsupportedOperationException(); }
    @Override public int getNumberOfDependents() { throw new UnsupportedOperationException(); }
    @Override public CompletableFuture<T> completeAsync(Supplier<? extends T> supplier, Executor executor) { throw new UnsupportedOperationException(); }
    @Override public CompletableFuture<T> completeAsync(Supplier<? extends T> supplier) { throw new UnsupportedOperationException(); }
    @Override public CompletableFuture<T> orTimeout(long timeout, TimeUnit unit) { throw new UnsupportedOperationException(); }
    @Override public CompletableFuture<T> completeOnTimeout(T value, long timeout, TimeUnit unit) { throw new UnsupportedOperationException(); }
    @Override public CompletableFuture<T> toCompletableFuture() { return super.thenApply(identity()); }

    boolean completeInternal(T value) {
        return super.complete(value);
    }

    boolean completeExceptionallyInternal(Throwable ex) {
        return super.completeExceptionally(ex);
    }
}
