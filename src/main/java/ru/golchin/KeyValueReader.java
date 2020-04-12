package ru.golchin;

import java.io.*;
import java.nio.file.Path;

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
        return read(true);
    }

    KeyValueRecord read(boolean movePointer) throws IOException {
        long start = getInputFile().getFilePointer();
        String key = readString();
        String value = readString();
        if (!movePointer)
            getInputFile().seek(start);
        return new KeyValueRecord(key, value);
    }

    String readValue(long offset) throws IOException {
        RandomAccessFile inputFile = getInputFile();
        inputFile.seek(offset);
        readString();
        return readString();
    }

    public boolean canRead() throws IOException {
        return getInputFile().getFilePointer() < getInputFile().length();
    }

    public long getOffset() throws IOException {
        return getInputFile().getFilePointer();
    }

    public void setOffset(long offset) throws IOException {
        getInputFile().seek(offset);
    }

    @Override
    public void close() throws IOException {
        getInputFile().close();
    }

}
