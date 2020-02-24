package ru.golchin;

import java.io.*;
import java.nio.file.Path;
import java.util.Map;

public class KeyValueReader implements Closeable {
    private final ThreadLocal<RandomAccessFile> inputFile;

    public KeyValueReader(Path path) {
        inputFile = ThreadLocal.withInitial(() -> {
            try {
                return new RandomAccessFile(path.toFile(), "r");
            } catch (FileNotFoundException e) {
                return null;
            }
        });
    }


    private RandomAccessFile getInputFile() {
        return inputFile.get();
    }

    String readString() throws IOException {
        int length = getInputFile().readInt();
        if (length < 0) {
            return null;
        }
        var bytes = new byte[2 * length];
        getInputFile().read(bytes, 0, bytes.length);
        var chars = new char[length];
        try (var dataInputStream = new DataInputStream(new ByteArrayInputStream(bytes))) {
            for (int i = 0; i < length; i++) {
                chars[i] = dataInputStream.readChar();
            }
        }
        return new String(chars);
    }

    KeyValueRecord read(long offset) throws IOException {
        getInputFile().seek(offset);
        return new KeyValueRecord(readString(), readString());
    }

    String readValue(long offset) throws IOException {
        RandomAccessFile inputFile = getInputFile();
        inputFile.seek(offset);
        readString();
        return readString();
    }

    @Override
    public void close() throws IOException {
        getInputFile().close();
    }

    static class KeyValueRecord {
        private String key;
        private String value;

        public KeyValueRecord(String key, String value) {
            this.key = key;
            this.value = value;
        }

        public String getKey() {
            return key;
        }

        public String getValue() {
            return value;
        }
    }
}
