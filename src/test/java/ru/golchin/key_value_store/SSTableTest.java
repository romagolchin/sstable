package ru.golchin.key_value_store;

public class SSTableTest extends AbstractBitcaskTest<SortedLogFile> {
    public SSTableTest() {
        logFileConstructor = SortedLogFile::new;
        mergeFunction = SortedLogFileMergeFunction.INSTANCE;
    }

}
