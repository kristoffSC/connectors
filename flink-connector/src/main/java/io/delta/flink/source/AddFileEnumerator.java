package io.delta.flink.source;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

public interface AddFileEnumerator<SplitT extends DeltaSourceSplit> {

    List<SplitT> enumerateSplits(AddFileEnumeratorContext context) throws IOException;

    // ------------------------------------------------------------------------

    @FunctionalInterface
    interface Provider<SplitT extends DeltaSourceSplit> extends Serializable {

        AddFileEnumerator<SplitT> create();
    }
}
