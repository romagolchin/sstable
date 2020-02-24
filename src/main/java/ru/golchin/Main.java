package ru.golchin;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class Main {
    public static void main(String[] args) throws IOException {
    }

    private static void readAndWrite() throws IOException {
        Path myTest = Paths.get("my_test");
        Random random = new Random();
        try (OutputStream outputStream = Files.newOutputStream(myTest);
             InputStream inputStream = Files.newInputStream(myTest)) {
            for (int i = 0; i < 100; i++) {
                byte[] bytes = new byte[16];
                random.nextBytes(bytes);
                System.out.println(Arrays.toString(bytes));
                outputStream.write(bytes);
//                outputStream.flush();
                byte[] readBytes = new byte[16];
                System.out.println(inputStream.read(readBytes));
                System.out.println(Arrays.toString(readBytes));
                assert Arrays.equals(bytes, readBytes);
            }

        }
    }
}
