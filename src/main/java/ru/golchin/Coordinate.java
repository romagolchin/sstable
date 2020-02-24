package ru.golchin;

public class Coordinate {
    public final int fileIndex;
    public final long offset;

    public Coordinate(int fileIndex, long offset) {
        this.fileIndex = fileIndex;
        this.offset = offset;
    }
}
