package ru.golchin.key_value_store;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import ru.golchin.util.PeekableIterator;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.*;

@SuppressWarnings("unused")
class LogFileTest extends CommonTest {
    @TempDir
    static Path tempDirectory;

    static List<LogFile> params() throws IOException {
        return List.of(new HashIndexLogFile(tempDirectory.resolve("hash")),
                new SortedLogFile(tempDirectory.resolve("sorted")));
    }

    @ParameterizedTest
    @MethodSource("params")
    void putGet(LogFile logFile) throws Exception {
        var list = generateKeysAndValues(4, 10, 1000);
        for (Map<String, String> entries : list) {
            for (Map.Entry<String, String> entry : entries.entrySet()) {
                String k = entry.getKey();
                String v = entry.getValue();
                logFile.put(k, v);
                assertEquals(v, logFile.get(k).getValue());
            }
        }
        logFile.closeOnWrite();
    }

    @Test
    void iterator() throws IOException {
        SortedLogFile file = new SortedLogFile(tempDirectory.resolve("file"));
        file.put("1", "1");
        file.put("2", "2");
        file.put("3", "3");
        file.closeOnWrite();
        PeekableIterator<KeyValueRecord> iterator = file.iterator();
        assertEquals("1", iterator.peek().getKey());
        assertEquals("1", iterator.next().getKey());
        assertEquals("2", iterator.next().getKey());
        assertEquals("2", iterator.next().getKey());
        iterator.peek();
        assertTrue(iterator.hasNext());
        assertEquals("3", iterator.next().getKey());
        assertFalse(iterator.hasNext());
        assertThrows(NoSuchElementException.class, iterator::next);
    }
}