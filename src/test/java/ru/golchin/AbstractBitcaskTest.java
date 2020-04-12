package ru.golchin;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

public abstract class AbstractBitcaskTest<T extends LogFile> extends AbstractTest {
    @TempDir
    public Path storePath;
    protected Bitcask<T> bitcask;
    protected ThrowingFunction<Path, T, IOException> logFileConstructor;
    protected MergeFunction<T> mergeFunction;

    @BeforeEach
    void setUp() throws Exception {
        bitcask = new Bitcask<>(storePath, HashIndexLogFile.MAX_SIZE, logFileConstructor, mergeFunction);
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
                bitcask.put(k, v);
                assertEquals(v, bitcask.get(k));
            }
        }
        for (Map.Entry<String, String> entry : resultingMap.entrySet()) {
            assertEquals(entry.getValue(), bitcask.get(entry.getKey()));
        }
        bitcask.close();
    }

    @Test
    void concurrent() throws InterruptedException {
        ExecutorService pool = Executors.newFixedThreadPool(10);
        pool.submit(() -> {
            try {
                bitcask.put("a", "1");
                bitcask.put("b", "2");
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        for (int i = 0; i < 10; i++) {
            pool.submit(() -> {
                try {
                    String b = bitcask.get("b");
                    String a = bitcask.get("a");
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
        assertNull(bitcask.get("cd"));
        bitcask.put("ab", "cde");
        bitcask.remove("ab");
        assertNull(bitcask.get("ab"));
    }

    @Test
    void recovery() throws Exception {
        var map = new HashMap<String, String>();
        for (int i = 0; i < 100; i++) {
            for (int j = 0; j < 10; j++) {
                String key = String.valueOf(i);
                String value = String.valueOf(i + j).repeat(1000);
                bitcask.put(key, value);
                map.put(key, value);
                bitcask.compact();
            }
        }
        bitcask.close();
        Bitcask<T> other = new Bitcask<>(storePath, HashIndexLogFile.MAX_SIZE, logFileConstructor, mergeFunction);
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
        System.out.println(Files.list(storePath).collect(Collectors.toList()));
        Files.delete(storePath);
    }
}
