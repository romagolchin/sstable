package ru.golchin.key_value_store;

public class SSTableTest extends CommonKeyValueStoreImplTest<SortedLogFile> {
    public SSTableTest() {
        logFileConstructor = SortedLogFile::new;
        mergeFunction = SortedLogFileMergeFunction.INSTANCE;
    }

}
