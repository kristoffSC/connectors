package io.delta.flink.source.internal.enumerator;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import io.delta.flink.source.internal.file.AddFileEnumerator;
import io.delta.flink.source.internal.file.AddFileEnumerator.SplitFilter;
import io.delta.flink.source.internal.file.AddFileEnumeratorContext;
import io.delta.flink.source.internal.state.DeltaSourceSplit;
import io.delta.flink.source.internal.utils.SourceUtils;
import org.apache.flink.core.fs.Path;

import io.delta.standalone.Snapshot;
import io.delta.standalone.actions.AddFile;

public class SnapshotProcessor {

    private final Path deltaTablePath;

    private final Snapshot snapshot;

    /**
     * The {@code AddFileEnumerator}'s to convert all discovered {@link AddFile} to set of {@link
     * DeltaSourceSplit}.
     */
    private final AddFileEnumerator<DeltaSourceSplit> fileEnumerator;

    /**
     * Set with already processed paths for Parquet Files. This map is used during recovery from
     * checkpoint.
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

    public List<DeltaSourceSplit> process() {
        // TODO Initial data read. This should be done in chunks since snapshot.getAllFiles()
        //  can have millions of files, and we would OOM the Job Manager
        //  if we would read all of them at once.
        AddFileEnumeratorContext context =
            setUpEnumeratorContext(snapshot.getAllFiles(), snapshot.getVersion());
        return fileEnumerator
            .enumerateSplits(context, (SplitFilter<Path>) alreadyProcessedPaths::add);
    }

    private AddFileEnumeratorContext setUpEnumeratorContext(List<AddFile> addFiles,
        long snapshotVersion) {
        String pathString = SourceUtils.pathToString(deltaTablePath);
        return new AddFileEnumeratorContext(pathString, addFiles, snapshotVersion);
    }

    public Collection<Path> getAlreadyProcessedPaths() {
        return alreadyProcessedPaths;
    }

    public long getSnapshotVersion() {
        return snapshot.getVersion();
    }
}
