package ru.golchin.key_value_store;

public class KeyValueStoreImplTest extends CommonKeyValueStoreImplTest<HashIndexLogFile> {
    public KeyValueStoreImplTest() {
        logFileConstructor = HashIndexLogFile::new;
        mergeFunction = HashIndexLogFileMergeFunction.INSTANCE;
    }

}
