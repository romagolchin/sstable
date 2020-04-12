package ru.golchin.util;

public interface ThrowingFunction<T, U, E extends Exception> {
    U apply(T t) throws E;
}
