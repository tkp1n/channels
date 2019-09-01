package com.ndportmann.channels;

import java.util.Deque;

/**
 * Specifies the behavior to use when writing to a bounded channel that is already full.
 */
public enum BoundedChannelFullMode {
    /**
     * Remove and ignore the newest item in the channel in order to make room for the item being written.
     */
    DROP_NEWEST {
        @Override
        final <T> boolean handleWriteOnBufferFull(T item, Deque<T> queue) {
            // drop newest and add current write
            queue.removeLast();
            queue.add(item);
            return true;
        }
    },
    /**
     * Remove and ignore the oldest item in the channel in order to make room for the item being written.
     */
    DROP_OLDEST {
        @Override
        final <T> boolean handleWriteOnBufferFull(T item, Deque<T> queue) {
            // drop oldest and add current write
            queue.removeFirst();
            queue.add(item);
            return true;
        }
    },
    /**
     * Drop the item being written.
     */
    DROP_WRITE {
        @Override
        final <T> boolean handleWriteOnBufferFull(T item, Deque<T> queue) {
            // Just ignore the write
            return true;
        }
    },
    /**
     * Wait for space to be available in order to complete the write operation.
     */
    WAIT {
        @Override
        final <T> boolean handleWriteOnBufferFull(T item, Deque<T> queue) {
            return false;
        }
    };

    abstract <T> boolean handleWriteOnBufferFull(final T item, Deque<T> queue);
}
