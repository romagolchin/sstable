package ru.golchin;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MappedLogFileTest extends AbstractTest {
    private MappedLogFile logFile;

    @BeforeEach
    void setUp() throws IOException {
        logFile = new MappedLogFile(Paths.get("my_test"));
    }

    @AfterEach
    void tearDown() throws IOException {
        logFile.closeOnWrite();
    }

    @Test
    void putGet() throws Exception {
        var list = generateKeysAndValues(4, 10, 100);
        for (Map<String, String> entries : list) {
            for (Map.Entry<String, String> entry : entries.entrySet()) {
                String k = entry.getKey();
                String v = entry.getValue();
                logFile.put(k, v);
                assertEquals(v, logFile.get(k));
            }
        }

    }
}