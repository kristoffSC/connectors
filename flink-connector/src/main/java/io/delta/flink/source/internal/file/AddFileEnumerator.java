package io.delta.flink.source.internal.file;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.function.Predicate;

import io.delta.flink.source.internal.state.DeltaSourceSplit;
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
