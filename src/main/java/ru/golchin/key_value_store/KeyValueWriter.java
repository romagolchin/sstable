package ru.golchin.key_value_store;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class KeyValueWriter implements Closeable {
    private final DataOutputStream outputStream;

    public KeyValueWriter(Path path) throws IOException {
        outputStream = new DataOutputStream(Files.newOutputStream(path, StandardOpenOption.CREATE, StandardOpenOption.APPEND));
    }

    void writeString(String s) throws IOException {
        if (s != null) {
            outputStream.writeInt(s.length());
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try (DataOutputStream inMemoryOutputStream = new DataOutputStream(out)) {
                inMemoryOutputStream.writeChars(s);
            }
            outputStream.write(out.toByteArray());
        } else {
            outputStream.writeInt(-1);
        }
    }

    int write(String key, String value) throws IOException {
        int offset = outputStream.size();
        writeString(key);
        writeString(value);
        return offset;
    }

    @Override
    public void close() throws IOException {
        outputStream.close();
    }

    private long getSize(String s) {
        return s == null ? 4 : s.length() * 2 + 4;
    }

    public long getRecordSize(String key, String value) {
        return getSize(key) + getSize(value);
    }
}
