package io.delta.flink.source.internal.enumerator.processor;

import io.delta.flink.source.internal.file.AddFileEnumerator.SplitFilter;
import org.apache.flink.core.fs.Path;

class CounterBasedSplitFilter implements SplitFilter<Path> {

    private final long offset;

    private long testCounter;

    CounterBasedSplitFilter(long offset) {
        this.offset = offset;
    }

    @Override
    public boolean test(Path path) {
        return ++testCounter > offset;
    }

    long getTestCounter() {
        return testCounter;
    }
}
