package ru.golchin;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class KeyValueWriter implements Closeable {
    private final DataOutputStream outputStream;
    private volatile long size;

    public KeyValueWriter(Path path) throws IOException {
        outputStream = new DataOutputStream(Files.newOutputStream(path, StandardOpenOption.CREATE, StandardOpenOption.APPEND));
        size = outputStream.size();
//        System.out.println(size);
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
        size = outputStream.size();
        return offset;
    }

    public long getSize() {
        return size;
    }

    @Override
    public void close() throws IOException {
        outputStream.close();
    }
}
