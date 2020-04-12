package ru.golchin;

import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.io.IOException;

public interface KeyValueStore<K, V> extends Closeable {
    void put(@NotNull K k, @NotNull V v) throws IOException;

    V get(@NotNull K k) throws IOException;

    void remove(@NotNull K k) throws IOException;
}
