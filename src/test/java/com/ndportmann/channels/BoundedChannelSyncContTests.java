package com.ndportmann.channels;

final class BoundedChannelSyncContTests extends BoundedChannelTests {
    BoundedChannelSyncContTests() {
        allowSynchronousContinuations = true;
    }
}
