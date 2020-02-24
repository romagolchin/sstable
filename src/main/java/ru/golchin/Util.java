package ru.golchin;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

public final class Util {
    private Util() {
    }

    public static void deleteDirectory(Path p) throws IOException {
        Files.list(p).forEach(path -> {
            try {
                Files.delete(path);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
        Files.delete(p);
    }
}
