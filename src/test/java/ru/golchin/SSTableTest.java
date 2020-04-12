package ru.golchin;

public class SSTableTest extends AbstractBitcaskTest<SortedLogFile> {
    public SSTableTest() {
        logFileConstructor = SortedLogFile::new;
        mergeFunction = SortedLogFileMergeFunction.INSTANCE;
    }

}
