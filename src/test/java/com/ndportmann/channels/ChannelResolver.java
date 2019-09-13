package com.ndportmann.channels;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.fail;

final class ChannelResolver implements ParameterResolver {
    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        return parameterContext.getParameter().getType().equals(Channel.class);
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        Object result = resolveSuper(extensionContext);
        if (result != null) {
            return result;
        }

        result = resolveEnclosing(extensionContext);
        if (result != null) {
            return result;
        }

        fail("This should never happen -- an implementation of createIntChannel() was not found");
        return null;
    }

    Object resolveSuper(ExtensionContext extensionContext) {
        Object testInstance = extensionContext.getTestInstance().get();
        try {
            Class<?> clazz = testInstance.getClass();
            do {
                try {
                    Method registry = clazz.getDeclaredMethod("createIntChannel");
                    registry.setAccessible(true);
                    return registry.invoke(testInstance);
                } catch (NoSuchMethodException ignored) {
                }

                clazz = clazz.getSuperclass();
            } while (clazz != null);
        } catch (IllegalAccessException | InvocationTargetException ignored) {
        }
        return null;
    }

    Object resolveEnclosing(ExtensionContext extensionContext) {
        Object o = extensionContext.getTestInstance().get();
        try {
            Class<?> clazz = o.getClass();
            Object target = o;
            do {
                try {
                    Method registry = clazz.getDeclaredMethod("createIntChannel");
                    registry.setAccessible(true);
                    return registry.invoke(target);
                } catch (NoSuchMethodException ignored) {
                }

                try {
                    target = o.getClass().getDeclaredField("this$0").get(target);
                } catch (NoSuchFieldException e) {
                    break;
                }
            } while ((clazz = clazz.getEnclosingClass()) != null);
        } catch (IllegalAccessException | InvocationTargetException e) {
            e.printStackTrace();
        }
        return null;
    }
}
