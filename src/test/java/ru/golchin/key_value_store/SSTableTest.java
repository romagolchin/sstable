package ru.golchin.key_value_store;

public class SSTableTest extends CommonKeyValueStoreImplTest<SSTableLogFile> {
    public SSTableTest() {
        logFileConstructor = SSTableLogFile::new;
        mergeFunction = SSTableMergeFunction.INSTANCE;
    }

}
