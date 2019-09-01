package com.ndportmann.channels;

import java.util.ArrayDeque;

final class BlockedWriterDeque extends ArrayDeque<Channel.WriteOperation> {
    final <T> Channel<T>.WriteOperation registerNewWriter(Channel<T> channel,  T item) {
        final var write = channel.newWriteOperation(item);
        add(write);
        return write;
    }

    final void writeOne() {
        final var blockedWrite = poll();
        if (blockedWrite != null) {
            blockedWrite.write();
        }
    }

    final void completeAll() {
        Channel.WriteOperation blockedWriter;
        while ((blockedWriter = poll()) != null) {
            blockedWriter.close();
        }
    }
}
