package ru.golchin;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HashIndexLogFileMergeFunction implements MergeFunction<HashIndexLogFile> {
    public static final HashIndexLogFileMergeFunction INSTANCE = new HashIndexLogFileMergeFunction();

    @Override
    public void merge(List<HashIndexLogFile> filesToCompact, HashIndexLogFile newFile) throws IOException {
        Map<String, HashIndexLogFile> keyToFile = new HashMap<>();
        for (var file : filesToCompact) {
            for (String key : file.getIndexKeys()) {
                keyToFile.put(key, file);
            }
        }
        for (var entry : keyToFile.entrySet()) {
            String key = entry.getKey();
            var file = entry.getValue();
            newFile.put(key, file.get(key).getValue());
        }
        newFile.closeOnWrite();
    }
}
