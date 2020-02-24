package ru.golchin;

import java.io.Closeable;
import java.io.IOException;

public interface KeyValueStore<K, V> extends Closeable {
    void put(K k, V v) throws IOException;

    V get(K k) throws IOException;

    void remove(K k) throws IOException;
}
