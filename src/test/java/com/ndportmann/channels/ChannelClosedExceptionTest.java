package com.ndportmann.channels;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ChannelClosedExceptionTest {

    @Test
    void ctors() {
        var e = new ChannelClosedException();
        assertNotNull(e.getMessage());
        assertNull(e.getCause());

        e = new ChannelClosedException("hello");
        assertEquals("hello", e.getMessage());
        assertNull(e.getCause());

        var inner = new IllegalStateException();
        e = new ChannelClosedException("hello", inner);
        assertEquals("hello", e.getMessage());
        assertEquals(inner, e.getCause());

        e = new ChannelClosedException(inner);
        assertNotNull(e.getMessage());
        assertEquals(inner, e.getCause());

        e = new ChannelClosedException("hello", inner, true, false);
        assertEquals("hello", e.getMessage());
        assertEquals(inner, e.getCause());
    }
}