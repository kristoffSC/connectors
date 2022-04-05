package io.delta.flink.source.internal.enumerator.processor;

import io.delta.flink.source.internal.file.AddFileEnumerator.SplitFilter;
import org.apache.flink.core.fs.Path;

/**
 * An implementation of {@link SplitFilter} that keeps track of number of test calls and compare it
 * with offset value used during objet initialization.
 */
class CounterBasedSplitFilter implements SplitFilter<Path> {

    /**
     * The upper limit under which every call to {@link #test(Path)} will return false.
     */
    private final long offset;

    /**
     * Total number of {@link #test(Path)} method calls.
     */
    private long testCounter;

    CounterBasedSplitFilter(long offset) {
        this.offset = offset;
    }

    /**
     * Evaluates {@link CounterBasedSplitFilter}.
     * <p>
     * In this implementation, evaluation is based on the result of the comparison {@link
     * #testCounter} and {@link #offset} values.
     * <p>
     * Each call to {@code test(path)} method increases {@link #testCounter} value by one.
     */
    @Override
    public boolean test(Path path) {
        return ++testCounter > offset;
    }

    /**
     * @return number of calls to {@link #test(Path)} method for this instance.
     */
    long getTestCounter() {
        return testCounter;
    }
}
