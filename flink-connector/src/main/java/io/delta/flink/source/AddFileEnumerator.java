package io.delta.flink.source;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.function.Predicate;

import org.apache.flink.core.fs.Path;

public interface AddFileEnumerator<SplitT extends DeltaSourceSplit> {

    List<SplitT> enumerateSplits(AddFileEnumeratorContext context, SplitFilter<Path> splitFilter)
        throws IOException;

    // ------------------------------------------------------------------------

    @FunctionalInterface
    interface Provider<SplitT extends DeltaSourceSplit> extends Serializable {

        AddFileEnumerator<SplitT> create();
    }

    @FunctionalInterface
    interface SplitFilter<T> extends Predicate<T>, Serializable {

    }
}
