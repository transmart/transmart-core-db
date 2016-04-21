package org.transmartproject.db.util

import org.hibernate.ScrollableResults
import org.transmartproject.core.IterableResult

class ResultIteratorWrappingIterable<T> extends AbstractOneTimeCallIterable<T> implements IterableResult<T> {

    protected final ResultIterator<T> closeableIterator

    ResultIteratorWrappingIterable(ScrollableResults scrollableResults) {
        this.closeableIterator = new ScrollableResultsIterator(scrollableResults)
    }

    ResultIteratorWrappingIterable(ResultIterator<T> closeableIterator) {
        this.closeableIterator = closeableIterator
    }

    static <T> ResultIteratorWrappingIterable<T> concat(Iterator<IterableResult<T>> results) {
        return new ResultIteratorWrappingIterable<T>(new IterableResultsConcat<T>(results))
    }

    @Override
    protected Iterator<T> getIterator() {
        closeableIterator
    }

    @Override
    void close() throws IOException {
        closeableIterator.close()
    }

}
