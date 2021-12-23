package io.delta.flink.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.PendingSplitsCheckpoint;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;
import org.apache.flink.connector.file.src.assigners.LocalityAwareSplitAssigner;
import org.apache.flink.connector.file.src.enumerate.FileEnumerator;
import org.apache.flink.connector.file.src.impl.FileSourceReader;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class DeltaSource<T>
    implements Source<T, FileSourceSplit, PendingSplitsCheckpoint<FileSourceSplit>>,
    ResultTypeQueryable<T> {

    public static final FileSplitAssigner.Provider DEFAULT_SPLIT_ASSIGNER =
        LocalityAwareSplitAssigner::new;

    public static final FileEnumerator.Provider DEFAULT_SPLITTABLE_FILE_ENUMERATOR =
        DeltaFileEnumerator::new;


    private static final long serialVersionUID = 1L;

    private final Path tablePath;

    private final BulkFormat<T, FileSourceSplit> readerFormat;

    private final SplitEnumeratorProvider splitEnumeratorProvider;

    protected DeltaSource(Path tablePath, BulkFormat<T, FileSourceSplit> readerFormat,
        SplitEnumeratorProvider splitEnumeratorProvider) {

        this.tablePath = tablePath;
        this.readerFormat = readerFormat;
        this.splitEnumeratorProvider = splitEnumeratorProvider;
    }

    /**
     * Builds a new {@code DeltaSource} using a {@link BulkFormat} to read batches of records from
     * files.
     *
     * <p>Examples for bulk readers are compressed and vectorized formats such as ORC or Parquet.
     */
    public static <T> DeltaSource<T> forBulkFileFormat(Path deltaTablePath,
        BulkFormat<T, FileSourceSplit> reader, SplitEnumeratorProvider splitEnumeratorProvider) {
        checkNotNull(deltaTablePath, "deltaTablePath");
        checkNotNull(reader, "reader");
        checkNotNull(splitEnumeratorProvider, "splitEnumeratorProvider");

        return new DeltaSource<>(deltaTablePath, reader, splitEnumeratorProvider);
    }

    @Override
    public SimpleVersionedSerializer<FileSourceSplit> getSplitSerializer() {
        return null;
    }

    @Override
    public SimpleVersionedSerializer<PendingSplitsCheckpoint<FileSourceSplit>>
        getEnumeratorCheckpointSerializer() {
        return null;
    }

    @Override
    public Boundedness getBoundedness() {
        return splitEnumeratorProvider.getBoundedness();
    }

    @Override
    public SourceReader<T, FileSourceSplit> createReader(SourceReaderContext readerContext)
        throws Exception {
        return new FileSourceReader<>(readerContext, readerFormat,
            readerContext.getConfiguration());
    }

    @Override
    public SplitEnumerator<FileSourceSplit, PendingSplitsCheckpoint<FileSourceSplit>>
        createEnumerator(
        SplitEnumeratorContext<FileSourceSplit> enumContext) {
        return splitEnumeratorProvider.createEnumerator(tablePath);
    }

    @Override
    public SplitEnumerator<FileSourceSplit, PendingSplitsCheckpoint<FileSourceSplit>>
        restoreEnumerator(SplitEnumeratorContext<FileSourceSplit> enumContext,
        PendingSplitsCheckpoint<FileSourceSplit> checkpoint) throws Exception {
        return splitEnumeratorProvider.createEnumerator(tablePath);
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return null;
    }
}
