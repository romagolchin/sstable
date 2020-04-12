package ru.golchin.key_value_store;

import org.jetbrains.annotations.NotNull;
import ru.golchin.util.ThrowingFunction;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static java.nio.file.Files.*;
import static java.util.stream.Collectors.toList;
import static ru.golchin.util.Util.deleteDirectory;

public class Bitcask<T extends LogFile> implements KeyValueStore<String, String> {
    public static final int MAX_FILES_TO_COMPACT = 4;
    private final Path directory;
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final ConcurrentNavigableMap<Integer, T> logFiles = new ConcurrentSkipListMap<>();
    private final AtomicInteger fileCounter;
    private final boolean shouldCompact;
    private ScheduledFuture<?> compacterScheduledFuture;
    private ScheduledExecutorService compacter;
    private volatile T currentFile;
    private final long maxSizeBytes;
    private final ThrowingFunction<Path, ? extends T, IOException> logFileConstructor;
    private final MergeFunction<T> mergeFunction;

    public Bitcask(Path directory,
                   long maxSizeBytes,
                   ThrowingFunction<Path, ? extends T, IOException> logFileConstructor,
                   MergeFunction<T> mergeFunction) throws IOException {
        this(directory, maxSizeBytes, logFileConstructor, mergeFunction, true);
    }

    public Bitcask(Path directory,
                   long maxSizeBytes,
                   ThrowingFunction<Path, ? extends T, IOException> logFileConstructor,
                   MergeFunction<T> mergeFunction, boolean shouldCompact) throws IOException {
        this.directory = directory;
        this.maxSizeBytes = maxSizeBytes;
        this.logFileConstructor = logFileConstructor;
        this.mergeFunction = mergeFunction;
        this.shouldCompact = shouldCompact;
        createDirectories(directory);
        for (Path path : newDirectoryStream(directory)) {
            if (isDirectory(path)) {
                T file = logFileConstructor.apply(path);
                logFiles.put(file.getVersion(), file);
            }
        }
        // initially files have odd numbers, files that result from compaction have even numbers
        // thus file that results from compaction has version number that is greater than all compacted files
        // but less than newer files
        int lastVersion = logFiles.isEmpty() ? -1 : nextOdd(logFiles.lastKey());
        fileCounter = new AtomicInteger(lastVersion);
        if (shouldCompact) {
            compacter = Executors.newScheduledThreadPool(1);
            compacterScheduledFuture = compacter.scheduleAtFixedRate(this::compact, 0, 10, TimeUnit.SECONDS);
        }
    }

    private int nextOdd(int n) {
        return (n & 1) == 1 ? n : n + 1;
    }

    @Override
    public void put(@NotNull String key, @NotNull String value) throws IOException {
        writeKeyValue(Objects.requireNonNull(key), Objects.requireNonNull(value));
    }

    private void writeKeyValue(String key, String value) throws IOException {
        readWriteLock.writeLock().lock();
        try {
            var logFile = getCurrentFile();
            logFile.put(key, value);
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    private T getCurrentFile() throws IOException {
        if (currentFile == null || exists(currentFile.getPath()) && currentFile.getSize() > maxSizeBytes) {
            replaceCurrentFile();
        }
        return currentFile;
    }

    private void replaceCurrentFile() throws IOException {
        if (currentFile != null) {
            currentFile.closeOnWrite();
            logFiles.put(currentFile.getVersion(), currentFile);
        }
        currentFile = createNewFile(fileCounter.addAndGet(2));
    }

    private T createNewFile(int version) throws IOException {
        Path currentFilePath = directory.resolve(String.valueOf(version));
        assert !exists(currentFilePath) : version;
        return logFileConstructor.apply(currentFilePath);
    }

    @Override
    public String get(@NotNull String key) throws IOException {
        Objects.requireNonNull(key);
        readWriteLock.readLock().lock();
        try {
            KeyValueRecord record = getCurrentFile().get(key);
            if (record != null)
                return record.getValue();
            for (var file : logFiles.descendingMap().values()) {
                record = file.get(key);
                if (record != null)
                    return record.getValue();
            }
            return null;
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    @Override
    public void remove(@NotNull String key) throws IOException {
        writeKeyValue(Objects.requireNonNull(key), null);
    }

    @Override
    public void close() throws IOException {
        currentFile.closeOnRead();
        currentFile.closeOnWrite();
        readWriteLock.readLock().lock();
        try {
            for (var logFile : logFiles.values()) {
                logFile.closeOnRead();
                logFile.closeOnWrite();
            }
        } finally {
            readWriteLock.readLock().unlock();
        }
        if (shouldCompact) {
            compacterScheduledFuture.cancel(false);
            compacter.shutdown();
            try {
                compacter.awaitTermination(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                compacter.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        assertIndices();
        assert exists(currentFile.getIndexPath()) : currentFile.getPath();
    }

    void compact() {
        readWriteLock.writeLock().lock();
        try {
            List<T> filesToCompact = logFiles.entrySet().stream()
                    .filter(v -> (v.getKey() % 2) == 1)
                    .limit(MAX_FILES_TO_COMPACT)
                    .map(Map.Entry::getValue)
                    .collect(toList());
            if (filesToCompact.size() < 2) {
                return;
            }
            int lastVersion = filesToCompact.get(filesToCompact.size() - 1).getVersion();
            T newFile = createNewFile(lastVersion + 1);
            List<Integer> versions = filesToCompact.stream().map(LogFile::getVersion).collect(toList());
            System.out.println("compacting versions " + versions + " to version " + newFile.getVersion());
            mergeFunction.merge(filesToCompact, newFile);
            logFiles.put(newFile.getVersion(), newFile);
            long sumSize = 0;
            for (var logFile : filesToCompact) {
                sumSize += logFile.getSize();
                logFiles.remove(logFile.getVersion());
                deleteDirectory(logFile.getPath());
            }
            System.out.println("before compaction " + sumSize);
            System.out.println("after compaction " + newFile.getSize());
            assertIndices();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    private void assertIndices() throws IOException {
        List<Path> badPaths = new ArrayList<>();
        Files.list(directory).forEach(path -> {
            try {
                if (isDirectory(path) && list(path).noneMatch(p -> "index".equals(p.getFileName().toString())))
                    badPaths.add(path);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        assert badPaths.size() < 2 : badPaths;
    }

}
