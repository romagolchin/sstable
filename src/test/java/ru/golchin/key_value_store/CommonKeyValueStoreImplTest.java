package ru.golchin.key_value_store;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.golchin.util.ThrowingFunction;
import ru.golchin.util.Util;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public abstract class CommonKeyValueStoreImplTest<T extends LogFile> extends CommonTest {
    @SuppressWarnings("unused")
    @TempDir
    public Path storePath;
    protected KeyValueStoreImpl<T> store;
    protected ThrowingFunction<Path, T, IOException> logFileConstructor;
    protected MergeFunction<T> mergeFunction;

    @BeforeEach
    void setUp() throws Exception {
        store = new KeyValueStoreImpl<>(storePath, HashIndexLogFile.MAX_SIZE, logFileConstructor, mergeFunction);
    }

    @Test
    void putGet() throws Exception {
        var resultingMap = new HashMap<String, String>();
        var list = generateKeysAndValues(10, 10, 100);
        for (Map<String, String> entries : list) {
            for (Map.Entry<String, String> entry : entries.entrySet()) {
                String k = entry.getKey();
                String v = entry.getValue();
                resultingMap.put(k, v);
                store.put(k, v);
                assertEquals(v, store.get(k));
            }
        }
        for (Map.Entry<String, String> entry : resultingMap.entrySet()) {
            assertEquals(entry.getValue(), store.get(entry.getKey()));
        }
        store.close();
    }

    @Test
    void concurrent() throws InterruptedException {
        ExecutorService pool = Executors.newFixedThreadPool(10);
        pool.submit(() -> {
            try {
                store.put("a", "1");
                store.put("b", "2");
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        for (int i = 0; i < 10; i++) {
            pool.submit(() -> {
                try {
                    String b = store.get("b");
                    String a = store.get("a");
                    assertTrue(b == null && a == null || "2".equals(b) && "1".equals(a));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }
        pool.shutdown();
        pool.awaitTermination(1, TimeUnit.SECONDS);
    }

    @Test
    void remove() throws Exception {
        assertNull(store.get("cd"));
        store.put("ab", "cde");
        store.remove("ab");
        assertNull(store.get("ab"));
    }

    @Test
    void recovery() throws Exception {
        var map = new HashMap<String, String>();
        for (int i = 0; i < 100; i++) {
            for (int j = 0; j < 10; j++) {
                String key = String.valueOf(i);
                String value = String.valueOf(i + j).repeat(1000);
                store.put(key, value);
                map.put(key, value);
                store.compact();
            }
        }
        store.close();
        KeyValueStoreImpl<T> other = new KeyValueStoreImpl<>(storePath, HashIndexLogFile.MAX_SIZE, logFileConstructor, mergeFunction);
        for (var entry : map.entrySet()) {
            other.compact();
            assertEquals(entry.getValue(), other.get(entry.getKey()));
        }
        other.close();
    }

    @AfterEach
    void tearDown() throws Exception {
        Files.list(storePath).forEach(p -> {
            try {
                Util.deleteDirectory(p);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
        Files.delete(storePath);
    }
}
