package ru.golchin.key_value_store;

class KeyValueStoreImplTest extends CommonKeyValueStoreImplTest<HashIndexLogFile> {
    public KeyValueStoreImplTest() {
        logFileConstructor = HashIndexLogFile::new;
        mergeFunction = HashIndexLogFileMergeFunction.INSTANCE;
    }

}