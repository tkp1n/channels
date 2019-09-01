package com.ndportmann.channels;

final class BoundedChannelAsyncContTests extends BoundedChannelTests {
    BoundedChannelAsyncContTests() {
        allowSynchronousContinuations = false;
    }
}
