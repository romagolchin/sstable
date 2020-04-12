package ru.golchin;

class BitcaskTest extends AbstractBitcaskTest<HashIndexLogFile> {
    public BitcaskTest() {
        logFileConstructor = HashIndexLogFile::new;
        mergeFunction = HashIndexLogFileMergeFunction.INSTANCE;
    }

}