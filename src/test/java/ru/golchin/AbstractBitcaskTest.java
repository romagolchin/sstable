package ru.golchin;

import org.junit.jupiter.api.BeforeEach;

import java.nio.file.Path;
import java.nio.file.Paths;

public abstract class AbstractBitcaskTest extends AbstractTest {
    static final int MAX_SIZE = 1024;
    public static final Path STORE_PATH = Paths.get("store");
    protected Bitcask bitcask;

    @BeforeEach
    void setUp() throws Exception {
        bitcask = new Bitcask(STORE_PATH, MAX_SIZE);
    }

}
