package io.delta.flink.source.internal.enumerator;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.function.Consumer;

import io.delta.flink.source.internal.file.AddFileEnumerator;
import io.delta.flink.source.internal.file.AddFileEnumerator.SplitFilter;
import io.delta.flink.source.internal.file.AddFileEnumeratorContext;
import io.delta.flink.source.internal.state.DeltaSourceSplit;
import io.delta.flink.source.internal.utils.SourceUtils;
import org.apache.flink.core.fs.Path;

import io.delta.standalone.Snapshot;
import io.delta.standalone.actions.AddFile;

/**
 * This implementation of {@link TableProcessor} process data from Delta table {@link Snapshot}.
 */
public class SnapshotProcessor implements TableProcessor {

    /**
     * A {@link Path} to Delta Table that this processor reads.
     */
    private final Path deltaTablePath;

    /**
     * A {@link Snapshot} that is processed by this processor.
     */
    private final Snapshot snapshot;

    /**
     * The {@code AddFileEnumerator}'s to convert all discovered {@link AddFile} to set of {@link
     * DeltaSourceSplit}.
     */
    private final AddFileEnumerator<DeltaSourceSplit> fileEnumerator;

    /**
     * Set with already processed paths for Parquet Files. Processor will skip not process parquet
     * files from this set.
     * <p>
     * The use case for this set is a recovery from checkpoint scenario, where we don't want to
     * reprocess already processed Parquet files.
     */
    private final HashSet<Path> alreadyProcessedPaths;

    public SnapshotProcessor(Path deltaTablePath, Snapshot snapshot,
        AddFileEnumerator<DeltaSourceSplit> fileEnumerator,
        Collection<Path> alreadyProcessedPaths) {
        this.deltaTablePath = deltaTablePath;
        this.snapshot = snapshot;
        this.fileEnumerator = fileEnumerator;
        this.alreadyProcessedPaths = new HashSet<>(alreadyProcessedPaths);
    }

    /**
     * Process all {@link AddFile} from {@link Snapshot} passed to this {@code SnapshotProcessor}
     * constructor by converting them to {@link DeltaSourceSplit} objects.
     *
     * @param processCallback A {@link Consumer} callback that will be called after converting all
     *                        {@link AddFile} to {@link DeltaSourceSplit}.
     */
    @Override
    public void process(Consumer<List<DeltaSourceSplit>> processCallback) {
        // TODO Initial data read. This should be done in chunks since snapshot.getAllFiles()
        //  can have millions of files, and we would OOM the Job Manager
        //  if we would read all of them at once.
        AddFileEnumeratorContext context =
            setUpEnumeratorContext(snapshot.getAllFiles(), snapshot.getVersion());
        List<DeltaSourceSplit> splits = fileEnumerator
            .enumerateSplits(context, (SplitFilter<Path>) alreadyProcessedPaths::add);

        processCallback.accept(splits);
    }

    /**
     * @return Collection of {@link Path} objects that corresponds to Parquet files processed by
     * this processor.
     */
    @Override
    public Collection<Path> getAlreadyProcessedPaths() {
        return alreadyProcessedPaths;
    }

    /**
     * @return A {@link Snapshot} version that this processor reads.
     */
    @Override
    public long getSnapshotVersion() {
        return snapshot.getVersion();
    }

    private AddFileEnumeratorContext setUpEnumeratorContext(List<AddFile> addFiles,
        long snapshotVersion) {
        String pathString = SourceUtils.pathToString(deltaTablePath);
        return new AddFileEnumeratorContext(pathString, addFiles, snapshotVersion);
    }
}
