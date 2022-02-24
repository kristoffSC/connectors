package io.delta.flink.source.internal.enumerator;

import java.io.Serializable;

import io.delta.flink.source.internal.DeltaSourceConfiguration;
import io.delta.flink.source.internal.state.DeltaEnumeratorStateCheckpoint;
import io.delta.flink.source.internal.state.DeltaSourceSplit;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.fs.Path;
import org.apache.hadoop.conf.Configuration;

/**
 * Factory for {@link SplitEnumerator}.
 */
public interface SplitEnumeratorProvider extends Serializable {

    /**
     * Creates {@link SplitEnumerator} instance.
     *
     * @param deltaTablePath      {@link Path} for Delta Table.
     * @param configuration       Hadoop Configuration that should be used to read Parquet files.
     * @param enumContext         {@link SplitEnumeratorContext}.
     * @param sourceConfiguration {@link DeltaSourceConfiguration} used for creating Delta Source.
     * @return {@link SplitEnumerator} instance.
     */
    SplitEnumerator<DeltaSourceSplit, DeltaEnumeratorStateCheckpoint<DeltaSourceSplit>>
        createEnumerator(Path deltaTablePath, Configuration configuration,
        SplitEnumeratorContext<DeltaSourceSplit> enumContext,
        DeltaSourceConfiguration sourceConfiguration);


    /**
     * Creates {@link SplitEnumerator} instance from {@link DeltaEnumeratorStateCheckpoint} data.
     *
     * @param checkpoint          {@link DeltaEnumeratorStateCheckpoint} that should be used to
     *                            create {@link SplitEnumerator} instance.
     * @param configuration       Hadoop Configuration that should be used to read Parquet files.
     * @param enumContext         {@link SplitEnumeratorContext}.
     * @param sourceConfiguration {@link DeltaSourceConfiguration} used for creating Delta Source.
     * @return {@link SplitEnumerator} instance.
     */
    SplitEnumerator<DeltaSourceSplit, DeltaEnumeratorStateCheckpoint<DeltaSourceSplit>>
        createEnumerator(
        DeltaEnumeratorStateCheckpoint<DeltaSourceSplit> checkpoint, Configuration configuration,
        SplitEnumeratorContext<DeltaSourceSplit> enumContext,
        DeltaSourceConfiguration sourceConfiguration);

    /**
     * @return {@link Boundedness} type for {@link SplitEnumerator} created by this provider.
     */
    Boundedness getBoundedness();

}
