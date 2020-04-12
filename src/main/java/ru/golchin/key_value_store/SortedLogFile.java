package ru.golchin.key_value_store;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

public class SortedLogFile extends LogFile {
    public static final int DEFAULT_SPARSE_FACTOR = 1000;
    private NavigableMap<String, Integer> keyToOffset;
    private final SortedMap<String, String> memTable = new TreeMap<>();
    private final int sparseFactor;
    private long sizeBytes = 0;

    public SortedLogFile(Path path) throws IOException {
        this(path, DEFAULT_SPARSE_FACTOR);
    }

    public SortedLogFile(Path path, int sparseFactor) throws IOException {
        super(path);
        this.sparseFactor = sparseFactor;
        if (keyToOffset == null) {
            keyToOffset = new TreeMap<>();
        }
    }

    @Override
    public KeyValueRecord get(String key) throws IOException {
        if (isClosedOnWrite) {
            Map.Entry<String, Integer> floorEntry = keyToOffset.floorEntry(key);
            if (floorEntry == null) {
                return null;
            }
            Integer lowOffset = floorEntry.getValue();
            KeyValueRecord record = keyValueReader.read(lowOffset);
            while (record != null && record.getKey().compareTo(key) < 0) {
                record = safeRead();
            }
            if (record != null && key.equals(record.getKey()))
                return record;
            return null;
        }
        if (!memTable.containsKey(key))
            return null;
        return new KeyValueRecord(key, memTable.get(key));
    }

    @Override
    public void put(String key, String value) {
        memTable.put(key, value);
        sizeBytes += keyValueWriter.getRecordSize(key, value);
    }

    @Override
    protected void restoreIndex() throws IOException {
        keyToOffset = new TreeMap<>(Objects.requireNonNull(readIndex()));
    }

    @Override
    public void dumpIndex() throws IOException {
        dumpIndex(sparsify(getIndex(), sparseFactor));
    }

    private Map<String, Integer> sparsify(Map<String, Integer> index, int sparseFactor) {
        int i = 0;
        var result = new TreeMap<String, Integer>();
        for (var entry : index.entrySet()) {
            if (i % sparseFactor == 0)
                result.put(entry.getKey(), entry.getValue());
            i++;
        }
        return result;
    }

    @Override
    protected Map<String, Integer> getIndex() {
        return keyToOffset;
    }

    @Override
    public void closeOnWrite() throws IOException {
        if (isClosedOnWrite)
            return;
        for (var entry : memTable.entrySet())
            super.put(entry.getKey(), entry.getValue());
        dumpIndex();
        keyValueWriter.close();
        isClosedOnWrite = true;
    }

    @Override
    public long getSize() {
        return sizeBytes;
    }
}
