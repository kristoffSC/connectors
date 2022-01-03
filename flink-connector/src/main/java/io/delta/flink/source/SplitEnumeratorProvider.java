package io.delta.flink.source;

import java.io.Serializable;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.fs.Path;
import org.apache.hadoop.conf.Configuration;

public interface SplitEnumeratorProvider extends Serializable {

    SplitEnumerator<DeltaSourceSplit, DeltaPendingSplitsCheckpoint<DeltaSourceSplit>>
        createEnumerator(Path deltaTablePath, Configuration configuration,
        SplitEnumeratorContext<DeltaSourceSplit> enumContext);

    Boundedness getBoundedness();

}
