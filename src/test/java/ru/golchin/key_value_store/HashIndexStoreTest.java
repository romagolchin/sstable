package ru.golchin.key_value_store;

public class HashIndexStoreTest extends CommonKeyValueStoreImplTest<HashIndexLogFile> {
    public HashIndexStoreTest() {
        logFileConstructor = HashIndexLogFile::new;
        mergeFunction = HashIndexLogFileMergeFunction.INSTANCE;
    }

}
