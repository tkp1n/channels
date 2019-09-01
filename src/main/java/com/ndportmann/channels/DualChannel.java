package com.ndportmann.channels;

public abstract class DualChannel<TRead, TWrite> {
    public abstract ChannelReader<TRead> reader();
    public abstract ChannelWriter<TWrite> writer();
}
