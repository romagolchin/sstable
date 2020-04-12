package ru.golchin;

import java.io.IOException;
import java.util.List;

public interface MergeFunction<T extends LogFile> {
    void merge(List<T> files, T newFile) throws IOException;
}
