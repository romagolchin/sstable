package ru.golchin.key_value_store;

class BitcaskTest extends AbstractBitcaskTest<HashIndexLogFile> {
    public BitcaskTest() {
        logFileConstructor = HashIndexLogFile::new;
        mergeFunction = HashIndexLogFileMergeFunction.INSTANCE;
    }

}