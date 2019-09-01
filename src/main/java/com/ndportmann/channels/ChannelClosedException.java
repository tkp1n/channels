package com.ndportmann.channels;

public final class ChannelClosedException extends RuntimeException {
    private static final String DEFAULT_MESSAGE = "Channel closed";

    ChannelClosedException() {
        super(DEFAULT_MESSAGE);
    }

    ChannelClosedException(String message) {
        super(message);
    }

    ChannelClosedException(String message, Throwable cause) {
        super(message, cause);
    }

    ChannelClosedException(Throwable cause) {
        super(DEFAULT_MESSAGE, cause);
    }

    ChannelClosedException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
