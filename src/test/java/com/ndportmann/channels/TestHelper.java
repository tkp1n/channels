package com.ndportmann.channels;

import org.junit.jupiter.api.function.Executable;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

final class TestHelper {
    TestHelper() {}

    static <T> T runOk(CompletionStage<T> asyncAction) {
        try {
            return asyncAction.toCompletableFuture().get();
        } catch (InterruptedException | ExecutionException ex) {
            fail(ex);
            return null;
        }
    }

    static <T> T run(Supplier<CompletionStage<T>> asyncAction) throws ExecutionException, InterruptedException {
        return asyncAction.get().toCompletableFuture().get();
    }

    static <T> Executable exec(Supplier<CompletionStage<T>> asyncAction) {
        return () -> run(asyncAction);
    }

    static <T extends Throwable> void assertThrowsCause(Class<T> expectedCauseType, Executable executable) {
        assertTrue(expectedCauseType.isInstance(assertThrows(ExecutionException.class, executable).getCause()));
    }

    static <T extends Throwable> void assertThrowsCause(T expectedCause, Executable executable) {
        assertEquals(expectedCause, (assertThrows(ExecutionException.class, executable).getCause()));
    }
}
