package ru.golchin;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static java.nio.file.Files.*;

class MappedLogFile {
    static final String DATA_FILE_NAME = "data";
    static final String INDEX_FILE_NAME = "index";
    private final Path path;
    private final Map<String, Integer> keyToOffset = new HashMap<>();
    private final KeyValueReader keyValueReader;
    private final KeyValueWriter keyValueWriter;

    MappedLogFile(Path path) throws IOException {
        this.path = path;
        boolean exists = exists(path);
        if (exists) {
            if (!isDirectory(path)) {
                throw new IllegalArgumentException();
            }
            keyToOffset.putAll(Objects.requireNonNull(readIndex()));
        } else {
            createDirectory(path);
        }
        keyValueWriter = new KeyValueWriter(path.resolve(DATA_FILE_NAME));
        if (exists) {
            keyValueWriter.close();
        }
        assert exists(path.resolve(DATA_FILE_NAME)) : path;
        keyValueReader = new KeyValueReader(path.resolve(DATA_FILE_NAME));
    }

    public static int getVersion(Path path) {
        return Integer.parseInt(path.getFileName().toString());
    }

    public Path getPath() {
        return path;
    }

    public int getVersion() {
        return getVersion(getPath());
    }

    String get(String key) throws IOException {
        Integer pos = keyToOffset.get(key);
        if (pos == null) {
            return null;
        }
        return keyValueReader.readValue(pos);
    }

    public void put(String key, String value) throws IOException {
        keyToOffset.put(key, keyValueWriter.write(key, value));
    }

    public void closeOnRead() throws IOException {
        keyValueReader.close();
    }

    public void closeOnWrite() throws IOException {
        keyValueWriter.close();
        dumpIndex();
    }

    Path getIndexPath() {
        return path.resolve(INDEX_FILE_NAME);
    }

    private Map<String, Integer> readIndex() throws IOException {
        Path path = getIndexPath();
        assert exists(path) : path + ", my path: " + getPath();
        try (ObjectInputStream ois = new ObjectInputStream(Files.newInputStream(path))) {
            return (Map<String, Integer>) ois.readObject();
        } catch (ClassNotFoundException ignored) {
            return null;
        }
    }

    public void dumpIndex() throws IOException {
        System.out.println("dump " + getIndexPath());
        try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(newOutputStream(getIndexPath()))) {
            objectOutputStream.writeObject(keyToOffset);
        }
        assert exists(getIndexPath());
    }

    public Set<String> getIndex() {
        return Collections.unmodifiableSet(keyToOffset.keySet());
    }

    public boolean containsKey(String key) {
        return keyToOffset.containsKey(key);
    }

    public long getSize() {
        return keyValueWriter.getSize();
    }
}

