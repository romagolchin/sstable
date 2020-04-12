package ru.golchin.key_value_store;

import ru.golchin.util.PeekableIterator;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static java.nio.file.Files.*;
import static java.util.stream.Collectors.toMap;

public abstract class LogFile {
    protected static final String DATA_FILE_NAME = "data";
    protected static final String INDEX_FILE_NAME = "index";
    protected final Path path;
    protected final KeyValueReader keyValueReader;
    protected final KeyValueWriter keyValueWriter;
    protected boolean isClosedOnWrite = false;

    public LogFile(Path path) throws IOException {
        this.path = path;
        boolean exists = exists(path);
        if (exists) {
            if (!isDirectory(path)) {
                throw new IllegalArgumentException();
            }
            restoreIndex();
        } else {
            createDirectory(path);
        }
        keyValueWriter = new KeyValueWriter(getDataPath());
        if (exists) {
            makeReadOnly();
        }
        keyValueReader = new KeyValueReader(getDataPath());
    }

    public void put(String key, String value) throws IOException {
        int offset = keyValueWriter.write(key, value);
        getIndex().put(key, offset);
    }

    public abstract KeyValueRecord get(String key) throws IOException;

    protected abstract void restoreIndex() throws IOException;

    public static int getVersion(Path path) {
        return Integer.parseInt(path.getFileName().toString());
    }

    public Path getPath() {
        return path;
    }

    public int getVersion() {
        return getVersion(getPath());
    }

    public void closeOnRead() throws IOException {
        keyValueReader.close();
    }

    public void closeOnWrite() throws IOException {
        keyValueWriter.close();
        dumpIndex();
    }

    public void makeReadOnly() throws IOException {
        keyValueWriter.close();
        isClosedOnWrite = true;
    }

    Path getIndexPath() {
        return path.resolve(INDEX_FILE_NAME);
    }

    private Path getDataPath() {
        return path.resolve(DATA_FILE_NAME);
    }

    @SuppressWarnings("unchecked")
    protected Map<String, Integer> readIndex() throws IOException {
        Path path = getIndexPath();
        assert exists(path) : "no index found in " + getPath();
        try (ObjectInputStream ois = new ObjectInputStream(Files.newInputStream(path))) {
            Object object = ois.readObject();
            return (Map<String, Integer>) object;
        } catch (ClassNotFoundException ignored) {
            return null;
        }
    }

    public void dumpIndex() throws IOException {
        dumpIndex(getIndex());
    }

    public void dumpIndex(Map<String, Integer> index) throws IOException {
        try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(newOutputStream(getIndexPath()))) {
            objectOutputStream.writeObject(index);
        }
    }

    protected abstract Map<String, Integer> getIndex();

    public long getSize() throws IOException {
        return size(getDataPath());
    }

    public KeyValueRecord safeRead() throws IOException {
        return safeRead(true);
    }

    public KeyValueRecord safeRead(boolean movePointer) throws IOException {
        if (keyValueReader.canRead())
            return keyValueReader.read(movePointer);
        return null;
    }

    List<KeyValueRecord> asRecordList() throws IOException {
        var offset = keyValueReader.getOffset();
        keyValueReader.setOffset(0);
        KeyValueRecord record;
        var records = new ArrayList<KeyValueRecord>();
        while ((record = safeRead()) != null)
            records.add(record);
        keyValueReader.setOffset(offset);
        return records;
    }

    Map<String, String> asMap() throws IOException {
        return asRecordList().stream()
                .collect(toMap(KeyValueRecord::getKey, KeyValueRecord::getValue));
    }

    PeekableIterator<KeyValueRecord> iterator() {
        return new LogFileIterator();
    }

    class LogFileIterator implements PeekableIterator<KeyValueRecord> {
        private final List<KeyValueRecord> record = new ArrayList<>();

        private KeyValueRecord throwIfNull(KeyValueRecord record) {
            if (record == null)
                throw new NoSuchElementException();
            return record;
        }

        @Override
        public KeyValueRecord peek() {
            assert record.size() <= 1;
            if (record.isEmpty()) {
                try {
                    record.add(safeRead());
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
            return record.get(0);
        }

        @Override
        public boolean hasNext() {
            assert record.size() <= 1;
            try {
                return !record.isEmpty() || keyValueReader.canRead();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public KeyValueRecord next() {
            assert record.size() <= 1;
            if (!record.isEmpty()) {
                KeyValueRecord record = this.record.get(0);
                this.record.clear();
                return throwIfNull(record);
            }
            try {
                KeyValueRecord record = safeRead();
                this.record.add(record);
                return throwIfNull(record);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
