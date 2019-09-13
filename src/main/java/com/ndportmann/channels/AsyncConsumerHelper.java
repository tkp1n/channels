package com.ndportmann.channels;

final class AsyncConsumerHelper {
    private AsyncConsumerHelper() {}

    @SuppressWarnings("unchecked")
    static <T extends Exception> void sneakyThrow(Throwable t) throws T {
        throw (T) t;
    }
}
