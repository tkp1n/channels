package com.ndportmann.channels;

import java.util.ArrayDeque;

final class BlockedReaderDeque<T> extends ArrayDeque<Channel<T>.ReadOperation> {
    final Channel<T>.ReadOperation registerNewReader(Channel<T> channel) {
        var read = channel.newReadOperation();
        add(read);
        return read;
    }

    final boolean readOne(T item) {
        final var blockedReader = poll();
        if (blockedReader != null) {
            blockedReader.read(item);
            return true;
        } else {
            return false;
        }
    }

    final void completeAll() {
        Channel.ReadOperation blockedReader;
        while ((blockedReader = poll()) != null) {
            blockedReader.close();
        }
    }
}
