package org.transmartproject.db.util

import com.google.common.collect.AbstractIterator
import org.transmartproject.core.IterableResult

/**
 * This class concatenates the provided {@Link IterableResult}s into a single ResultIterator.
 * If the collection of IterableResults itself is also {@Link Closeable} its close() method will be called when this
 * iterator is exhausted or closed explicitly.
 *
 * @param <T>
 */
class IterableResultsConcat<T> extends AbstractIterator<T> implements ResultIterator<T> {

    private Iterator<IterableResult<T>> results
    private IterableResult<T> currentResult
    private Iterator<T> current

    IterableResultsConcat(Iterator<IterableResult<T>> results) {
        this.results = results
    }
    IterableResultsConcat(Iterable<IterableResult<T>> results) {
        this(results.iterator())
    }

    T computeNext() {
        while(current == null || !current.hasNext()) {
            currentResult?.close()
            if(!results.hasNext()) {
                closeResults()
                return endOfData()
            }
            currentResult = results.next()
            current = currentResult.iterator()
        }

        return current.next()
    }

    void close() {
        currentResult?.close()
        closeResults()
    }

    private void closeResults() {
        if(results instanceof Closeable) {
            ((Closeable) results).close()
        }
    }
}
