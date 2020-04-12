package ru.golchin;

import java.io.IOException;
import java.util.List;

import static java.util.stream.Collectors.toList;

public class SortedLogFileMergeFunction implements MergeFunction<SortedLogFile> {
    public static final SortedLogFileMergeFunction INSTANCE = new SortedLogFileMergeFunction();

    KeyValueRecord ceilingRecord(PeekableIterator<KeyValueRecord> logFileIterator, String key) {
        KeyValueRecord record = logFileIterator.peek();
        if (key == null || record == null) {
            return record;
        }
        var found = false;
        while (!found) {
            if (record == null || record.getKey().compareTo(key) > 0)
                found = true;
            else if (logFileIterator.hasNext())
                record = logFileIterator.next();
            else {
                return record.getKey().compareTo(key) > 0 ? record : null;
            }
        }
        return record;
    }

    @Override
    public void merge(List<SortedLogFile> files, SortedLogFile newFile) throws IOException {
        String lastWrittenKey = null;
        List<PeekableIterator<KeyValueRecord>> records = files.stream()
                .map(LogFile::iterator).collect(toList());
        KeyValueRecord minKeyRecord;
        do {
            minKeyRecord = null;
            for (int i = 0; i < files.size(); i++) {
                PeekableIterator<KeyValueRecord> peekableIterator = records.get(i);
                KeyValueRecord curRecord = ceilingRecord(peekableIterator, lastWrittenKey);
                if (curRecord != null) {
                    if (minKeyRecord == null) {
                        minKeyRecord = curRecord;
                    } else {
                        var minKey = minKeyRecord.getKey();
                        var curKey = curRecord.getKey();
                        if (curKey.compareTo(minKey) <= 0) {
                            minKeyRecord = curRecord;
                        }
                    }
                }
            }
            if (minKeyRecord != null) {
                newFile.put(minKeyRecord.getKey(), minKeyRecord.getValue());
                lastWrittenKey = minKeyRecord.getKey();
            }
        } while (minKeyRecord != null);
        newFile.closeOnWrite();
    }
}
