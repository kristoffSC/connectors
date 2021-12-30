package io.delta.flink.source;

import java.io.Serializable;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.fs.Path;

public interface SplitEnumeratorProvider extends Serializable {

    SplitEnumerator<DeltaSourceSplit, DeltaPendingSplitsCheckpoint<DeltaSourceSplit>>
        createEnumerator(Path deltaTablePath, SplitEnumeratorContext<DeltaSourceSplit> enumContext);

    Boundedness getBoundedness();

}
