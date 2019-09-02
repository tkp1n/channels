package com.ndportmann.channels;

public abstract class DualChannel<TRead, TWrite> {
    /**
     * Returns the readable half of a {@link DualChannel}
     * @return The readable half of a {@link DualChannel}
     */
    public abstract ChannelReader<TRead> reader();

    /**
     * Returns the writable half of a {@link DualChannel}
     * @return The writable half of a {@link DualChannel}
     */
    public abstract ChannelWriter<TWrite> writer();
}
