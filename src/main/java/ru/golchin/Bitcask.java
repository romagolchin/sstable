package ru.golchin;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static java.nio.file.Files.*;
import static ru.golchin.Util.deleteDirectory;

public class Bitcask implements KeyValueStore<String, String> {
    public static final int MAX_FILES_TO_COMPACT = 4;
    private final Path directory;
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final ConcurrentNavigableMap<Integer, MappedLogFile> logFiles = new ConcurrentSkipListMap<>();
    private final AtomicInteger fileCounter;
    private final ScheduledFuture<?> compacterScheduledFuture;
    private final ScheduledExecutorService compacter;
    private volatile MappedLogFile currentFile;
    private final long maxSizeBytes;

    public Bitcask(Path directory, long maxSizeBytes) throws IOException {
        this.directory = directory;
        this.maxSizeBytes = maxSizeBytes;
        createDirectories(directory);
        for (Path path : newDirectoryStream(directory)) {
            if (isDirectory(path)) {
                logFiles.put(MappedLogFile.getVersion(path), new MappedLogFile(path));
            }
        }
        // initially files have odd numbers, files that result from compaction have even numbers
        // thus file that results from compaction has version number that is greater than all compacted files
        // but less than newer files
        int lastVersion = logFiles.isEmpty() ? -1 : nextOdd(logFiles.lastKey());
        fileCounter = new AtomicInteger(lastVersion);
        compacter = Executors.newScheduledThreadPool(1);
        compacterScheduledFuture = compacter.scheduleAtFixedRate(this::compact, 0, 10, TimeUnit.SECONDS);
    }

    private int nextOdd(int n) {
        return (n & 1) == 1 ? n : n + 1;
    }

    @Override
    public void put(String key, String value) throws IOException {
        writeKeyValue(key, value);
    }

    private void writeKeyValue(String key, String value) throws IOException {
        readWriteLock.writeLock().lock();
        try {
            MappedLogFile logFile = getCurrentFile();
            logFile.put(key, value);
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    private MappedLogFile getCurrentFile() throws IOException {
        if (currentFile == null || exists(currentFile.getPath()) && currentFile.getSize() > maxSizeBytes) {
            replaceCurrentFile();
        }
        return currentFile;
    }

    private void replaceCurrentFile() throws IOException {
        if (currentFile != null) {
            currentFile.closeOnWrite();
            logFiles.put(MappedLogFile.getVersion(currentFile.getPath()), currentFile);
        }
        currentFile = createNewFile(fileCounter.addAndGet(2));
    }

    private MappedLogFile createNewFile(int version) throws IOException {
        Path currentFilePath = directory.resolve(String.valueOf(version));
        assert !exists(currentFilePath) : version;
        return new MappedLogFile(currentFilePath);
    }

    @Override
    public String get(String key) throws IOException {
        readWriteLock.readLock().lock();
        try {
            if (getCurrentFile().containsKey(key)) {
                return currentFile.get(key);
            }
            for (MappedLogFile file : logFiles.descendingMap().values()) {
                if (file.containsKey(key)) {
                    return file.get(key);
                }
            }
            return null;
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    @Override
    public void remove(String key) throws IOException {
        writeKeyValue(key, null);
    }

    @Override
    public void close() throws IOException {
        currentFile.closeOnRead();
        currentFile.closeOnWrite();
        readWriteLock.readLock().lock();
        try {
            for (MappedLogFile logFile : logFiles.values()) {
                logFile.closeOnRead();
                logFile.closeOnWrite();
            }
        } finally {
            readWriteLock.readLock().unlock();
        }
        compacterScheduledFuture.cancel(false);
        compacter.shutdown();
        try {
            compacter.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            compacter.shutdownNow();
            Thread.currentThread().interrupt();
        }
        assertIndexes();
        assert exists(currentFile.getIndexPath()) : currentFile.getPath();
    }

    void compact(Collection<MappedLogFile> filesToCompact, MappedLogFile newFile) throws IOException {
        Map<String, MappedLogFile> keyToFile = new HashMap<>();
        for (MappedLogFile file : filesToCompact) {
            for (String key : file.getIndex()) {
                keyToFile.put(key, file);
            }
        }
        System.out.println("compacting versions " + filesToCompact + " to version " + newFile.getVersion());
        for (var entry : keyToFile.entrySet()) {
            String key = entry.getKey();
            MappedLogFile file = entry.getValue();
            newFile.put(key, file.get(key));
        }
        newFile.closeOnWrite();
    }

    void compact() {
        readWriteLock.writeLock().lock();
        try {
            List<MappedLogFile> filesToCompact = logFiles.entrySet().stream()
                    .filter(v -> (v.getKey() % 2) == 1)
                    .limit(MAX_FILES_TO_COMPACT)
                    .map(Map.Entry::getValue)
                    .collect(Collectors.toList());
            if (filesToCompact.size() < 2) {
                return;
            }
            int lastVersion = filesToCompact.get(filesToCompact.size() - 1).getVersion();
            MappedLogFile newFile = createNewFile(lastVersion + 1);
            compact(filesToCompact, newFile);
            logFiles.put(newFile.getVersion(), newFile);
            long sumSize = 0;
            for (MappedLogFile logFile : filesToCompact) {
                sumSize += logFile.getSize();
                logFiles.remove(logFile.getVersion());
                deleteDirectory(logFile.getPath());
            }
            System.out.println("before compaction " + sumSize);
            System.out.println("after compaction " + newFile.getSize());
            assertIndexes();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    private void assertIndexes() throws IOException {
        List<Path> badPaths = new ArrayList<>();
        Files.list(directory).forEach(path -> {
            try {
                if (isDirectory(path) && list(path).noneMatch(p -> "index".equals(p.getFileName().toString())))
                    badPaths.add(path);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        System.out.println(badPaths.size() + " paths");
        assert badPaths.size() < 2 : badPaths;
    }

}
