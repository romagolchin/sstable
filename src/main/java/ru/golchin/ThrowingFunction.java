package ru.golchin;

public interface ThrowingFunction<T, U, E extends Exception> {
    U apply(T t) throws E;
}
