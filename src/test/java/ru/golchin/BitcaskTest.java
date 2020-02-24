package ru.golchin;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class BitcaskTest extends AbstractBitcaskTest {

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
                System.out.println("put");
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
                    System.out.println(b + " " + a);
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
        for (int i = 0; i < 20; i++) {
            String key = String.valueOf(i);
            String value = String.valueOf(i).repeat(1000);
            bitcask.put(key, value);
            map.put(key, value);
        }
        bitcask.compact();
        bitcask.close();
        Bitcask other = new Bitcask(STORE_PATH, MAX_SIZE);
        for (var entry : map.entrySet()) {
            other.compact();
            assertEquals(entry.getValue(), other.get(entry.getKey()));
        }
        other.close();
    }

    @AfterEach
    void tearDown() throws Exception {
        Files.list(STORE_PATH).forEach(p -> {
            try {
                Util.deleteDirectory(p);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
        System.out.println(Files.list(STORE_PATH).collect(Collectors.toList()));
        Files.delete(STORE_PATH);
    }

}