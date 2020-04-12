package ru.golchin;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class SortedLogFileMergeFunctionTest {
    @TempDir
    Path path;

    private static Stream<Arguments> params() {
        return Stream.of(
                arguments(List.of(Map.of(), Map.of()), Map.of()),
                arguments(List.of(Map.of("09", "a"), Map.of()), new TreeMap<>(Map.of("09", "a"))),
                arguments(
                        List.of(Map.of("09", "a", "10", "b"), Map.of("10", "c")),
                        new TreeMap<>(Map.of("09", "a", "10", "c"))),
                arguments(List.of(
                        Map.of("09", "a", "10", "b"),
                        Map.of("08", "c", "09", "d"),
                        Map.of("07", "e", "08", "f")),
                        Map.of("07", "e", "08", "f", "09", "d", "10", "b"))
        );
    }

    @ParameterizedTest
    @MethodSource("params")
    void merge(List<Map<String, String>> sourceMaps, Map<String, String> mergedRecords) throws IOException {
        var files = new ArrayList<SortedLogFile>();
        for (int i = 0; i < sourceMaps.size(); i++) {
            Path logPath = path.resolve(String.valueOf(i));
            SortedLogFile logFile = new SortedLogFile(logPath);
            files.add(logFile);
            Map<String, String> sourceMap = sourceMaps.get(i);
            for (var entry : sourceMap.entrySet()) {
                logFile.put(entry.getKey(), entry.getValue());
            }
            logFile.closeOnWrite();
        }
        SortedLogFile newFile = new SortedLogFile(path.resolve(String.valueOf(sourceMaps.size())));
        SortedLogFileMergeFunction.INSTANCE.merge(files, newFile);
        assertEquals(mergedRecords, newFile.asMap());
    }
}