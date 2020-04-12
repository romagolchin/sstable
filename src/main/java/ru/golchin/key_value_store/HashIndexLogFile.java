package ru.golchin.key_value_store;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

public class HashIndexLogFile extends LogFile {
    static final int MAX_SIZE = 8 * 1024;
    private Map<String, Integer> keyToOffset;

    public HashIndexLogFile(Path path) throws IOException {
        super(path);
        if (keyToOffset == null) {
            keyToOffset = new HashMap<>();
        }
    }

    @Override
    protected void restoreIndex() throws IOException {
        keyToOffset = Objects.requireNonNull(readIndex());
    }

    public KeyValueRecord get(String key) throws IOException {
        Integer pos = keyToOffset.get(key);
        if (pos == null) {
            return null;
        }
        return keyValueReader.read(pos);
    }

    public Set<String> getIndexKeys() {
        return Collections.unmodifiableSet(keyToOffset.keySet());
    }

    @Override
    protected Map<String, Integer> getIndex() {
        return keyToOffset;
    }

}

