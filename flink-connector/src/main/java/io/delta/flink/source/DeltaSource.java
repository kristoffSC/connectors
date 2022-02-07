package io.delta.flink.source;

import io.delta.flink.source.internal.DeltaSourceOptions;
import io.delta.flink.source.internal.enumerator.SplitEnumeratorProvider;
import io.delta.flink.source.internal.state.DeltaEnumeratorStateCheckpoint;
import io.delta.flink.source.internal.state.DeltaSourceSplit;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.connector.file.src.impl.FileSourceReader;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.formats.parquet.utils.SerializableConfiguration;
import org.apache.hadoop.conf.Configuration;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class DeltaSource<T>
    implements Source<T, DeltaSourceSplit, DeltaEnumeratorStateCheckpoint<DeltaSourceSplit>>,
    ResultTypeQueryable<T> {

    // ---------------------------------------------------------------------------------------------
    // ALL NON TRANSIENT FIELDS HAVE TO BE SERIALIZABLE
    // ---------------------------------------------------------------------------------------------
    private static final long serialVersionUID = 1L;

    private final Path tablePath;

    private final BulkFormat<T, DeltaSourceSplit> readerFormat;

    private final SplitEnumeratorProvider splitEnumeratorProvider;

    private final SerializableConfiguration serializableConf;

    private final DeltaSourceOptions sourceOptions;

    // ---------------------------------------------------------------------------------------------

    protected DeltaSource(Path tablePath, BulkFormat<T, DeltaSourceSplit> readerFormat,
        SplitEnumeratorProvider splitEnumeratorProvider, Configuration configuration,
        DeltaSourceOptions sourceOptions) {

        this.tablePath = tablePath;
        this.readerFormat = readerFormat;
        this.splitEnumeratorProvider = splitEnumeratorProvider;
        this.serializableConf = new SerializableConfiguration(configuration);
        this.sourceOptions = sourceOptions;
    }

    /**
     * Builds a new {@code DeltaSource} using a {@link BulkFormat} to read batches of records from
     * files.
     *
     * <p>Examples for bulk readers are compressed and vectorized formats such as ORC or Parquet.
     */
    public static <T> DeltaSource<T> forBulkFileFormat(Path deltaTablePath,
        BulkFormat<T, DeltaSourceSplit> reader, SplitEnumeratorProvider splitEnumeratorProvider,
        Configuration configuration, DeltaSourceOptions sourceOptions) {
        checkNotNull(deltaTablePath, "deltaTablePath");
        checkNotNull(reader, "reader");
        checkNotNull(splitEnumeratorProvider, "splitEnumeratorProvider");

        return new DeltaSource<>(deltaTablePath, reader, splitEnumeratorProvider, configuration,
            sourceOptions);
    }

    @Override
    public SimpleVersionedSerializer<DeltaSourceSplit> getSplitSerializer() {
        // TODO Add in PR_2
        return null;
    }

    @Override
    public SimpleVersionedSerializer<DeltaEnumeratorStateCheckpoint<DeltaSourceSplit>>
        getEnumeratorCheckpointSerializer() {
        // TODO Add in PR_2
        return null;
    }

    @Override
    public Boundedness getBoundedness() {
        return splitEnumeratorProvider.getBoundedness();
    }

    @Override
    public SourceReader<T, DeltaSourceSplit> createReader(SourceReaderContext readerContext)
        throws Exception {
        return new FileSourceReader<>(readerContext, readerFormat,
            readerContext.getConfiguration());
    }

    @Override
    public SplitEnumerator<DeltaSourceSplit, DeltaEnumeratorStateCheckpoint<DeltaSourceSplit>>
        createEnumerator(
        SplitEnumeratorContext<DeltaSourceSplit> enumContext) {
        return splitEnumeratorProvider.createEnumerator(tablePath, serializableConf.conf(),
            enumContext, sourceOptions);
    }

    @Override
    public SplitEnumerator<DeltaSourceSplit, DeltaEnumeratorStateCheckpoint<DeltaSourceSplit>>
        restoreEnumerator(SplitEnumeratorContext<DeltaSourceSplit> enumContext,
        DeltaEnumeratorStateCheckpoint<DeltaSourceSplit> checkpoint) throws Exception {

        return splitEnumeratorProvider.createEnumerator(
            checkpoint, serializableConf.conf(), enumContext, sourceOptions);
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return readerFormat.getProducedType();
    }

    @VisibleForTesting
    Path getTablePath() {
        return tablePath;
    }
}
