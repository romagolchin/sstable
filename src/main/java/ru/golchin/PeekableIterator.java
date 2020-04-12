package ru.golchin;

import java.util.Iterator;

public interface PeekableIterator<E> extends Iterator<E> {
    /**
     * Returns what {@link Iterator#next()} would return but doesn't change position.
     * It is not required for the method to throw {@link java.util.NoSuchElementException}.
     */
    E peek();
}
