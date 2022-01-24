package io.delta.flink.source;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import javax.annotation.Nullable;

import io.delta.flink.source.enumerator.BoundedDeltaSourceSplitEnumerator;
import io.delta.flink.source.file.AddFileEnumeratorContext;
import io.delta.flink.source.state.DeltaEnumeratorStateCheckpoint;
import io.delta.flink.source.state.DeltaSourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;
import org.apache.flink.core.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static io.delta.flink.source.DeltaSourceSplitEnumerator.AssignSplitStatus.NO_MORE_READERS;
import static io.delta.flink.source.DeltaSourceSplitEnumerator.AssignSplitStatus.NO_MORE_SPLITS;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;
import io.delta.standalone.actions.AddFile;

public abstract class DeltaSourceSplitEnumerator implements
    SplitEnumerator<DeltaSourceSplit, DeltaEnumeratorStateCheckpoint<DeltaSourceSplit>> {

    protected static final int NO_SNAPSHOT_VERSION = -1;
    private static final Logger LOG =
        LoggerFactory.getLogger(BoundedDeltaSourceSplitEnumerator.class);
    protected final Path deltaTablePath;
    protected final FileSplitAssigner splitAssigner;
    protected final Snapshot snapshot;
    protected final SplitEnumeratorContext<DeltaSourceSplit> enumContext;
    protected final LinkedHashMap<Integer, String> readersAwaitingSplit;
    protected final HashSet<Path> pathsAlreadyProcessed;
    protected final DeltaLog deltaLog;
    protected final long initialSnapshotVersion;
    protected final DeltaSourceOptions sourceOptions;

    public DeltaSourceSplitEnumerator(
        Path deltaTablePath, FileSplitAssigner splitAssigner, Configuration configuration,
        SplitEnumeratorContext<DeltaSourceSplit> enumContext, DeltaSourceOptions sourceOptions) {
        this(deltaTablePath, splitAssigner, configuration, enumContext, sourceOptions,
            NO_SNAPSHOT_VERSION, Collections.emptySet());
    }

    public DeltaSourceSplitEnumerator(
        Path deltaTablePath, FileSplitAssigner splitAssigner, Configuration configuration,
        SplitEnumeratorContext<DeltaSourceSplit> enumContext, DeltaSourceOptions sourceOptions,
        long initialSnapshotVersion, Collection<Path> alreadyDiscoveredPaths) {
        this.splitAssigner = splitAssigner;
        this.enumContext = enumContext;
        this.readersAwaitingSplit = new LinkedHashMap<>();
        this.deltaTablePath = deltaTablePath;
        this.sourceOptions = sourceOptions;

        this.deltaLog =
            DeltaLog.forTable(configuration, deltaTablePath.toUri().normalize().toString());
        this.snapshot = (initialSnapshotVersion == NO_SNAPSHOT_VERSION) ?
            deltaLog.snapshot() : deltaLog.getSnapshotForVersionAsOf(initialSnapshotVersion);

        this.initialSnapshotVersion = snapshot.getVersion();
        this.pathsAlreadyProcessed = new HashSet<>(alreadyDiscoveredPaths);
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        if (!enumContext.registeredReaders().containsKey(subtaskId)) {
            // reader failed between sending the request and now. skip this request.
            return;
        }

        if (LOG.isInfoEnabled()) {
            final String hostInfo =
                requesterHostname == null ? "(no host locality info)"
                    : "(on host '" + requesterHostname + "')";
            LOG.info("Subtask {} {} is requesting a file source split", subtaskId, hostInfo);
        }

        readersAwaitingSplit.put(subtaskId, requesterHostname);
        assignSplits(subtaskId);
    }

    @Override
    public void addSplitsBack(List<DeltaSourceSplit> splits, int subtaskId) {
        LOG.debug("Bounded Delta Source Enumerator adds splits back: {}", splits);
        addSplits(splits);
    }

    @Override
    public void addReader(int subtaskId) {
        // this source is purely lazy-pull-based, nothing to do upon registration
    }

    @Override
    public void close() throws IOException {
        // no resources to close
    }

    @SuppressWarnings("unchecked")
    protected Collection<DeltaSourceSplit> getRemainingSplits() {
        // The Flink's SplitAssigner interface uses FileSourceSplit
        // in its signatures and return types even though it is expected to be extended
        // by other implementations. This "trick" is used also in Flink source code
        // by bundled Hive connector -
        // https://github.com/apache/flink/blob/release-1.14/flink-connectors/flink-connector-hive/src/main/java/org/apache/flink/connectors/hive/ContinuousHiveSplitEnumerator.java#L137
        return (Collection<DeltaSourceSplit>) (Collection<?>) splitAssigner.remainingSplits();
    }

    protected void addSplits(List<DeltaSourceSplit> splits) {
        // We are creating new Array to trick Java type check since the signature of this
        // constructor is "? extends E". The downside is that this constructor creates an extra
        // array and copies all elements which is not efficient.
        // However, there is no point for construction our custom Interface and Implementation
        // for splitAssigner just to have needed type.
        splitAssigner.addSplits(new ArrayList<>(splits));
    }

    protected AddFileEnumeratorContext setUpEnumeratorContext(List<AddFile> addFiles) {
        String pathString = SourceUtils.pathToString(deltaTablePath);
        return new AddFileEnumeratorContext(pathString, addFiles);
    }

    protected abstract void handleNoMoreSplits(int subtaskId);

    protected AssignSplitStatus assignSplits() {
        final Iterator<Entry<Integer, String>> awaitingReader =
            readersAwaitingSplit.entrySet().iterator();

        while (awaitingReader.hasNext()) {
            Entry<Integer, String> nextAwaiting = awaitingReader.next();

            // if the reader that requested another split has failed in the meantime, remove
            // it from the list of waiting readers - FLINK-20261
            if (!enumContext.registeredReaders().containsKey(nextAwaiting.getKey())) {
                awaitingReader.remove();
                continue;
            }

            String hostname = nextAwaiting.getValue();
            int awaitingSubtask = nextAwaiting.getKey();
            Optional<FileSourceSplit> nextSplit = splitAssigner.getNext(hostname);
            if (nextSplit.isPresent()) {
                FileSourceSplit split = nextSplit.get();
                enumContext.assignSplit((DeltaSourceSplit) split, awaitingSubtask);
                LOG.info("Assigned split to subtask {} : {}", awaitingSubtask, split);
                awaitingReader.remove();
            } else {
                // TODO for chunking load we will have to modify this to get a new chunk from Delta.
                return NO_MORE_SPLITS;
            }
        }

        return NO_MORE_READERS;
    }

    private void assignSplits(int subtaskId) {
        AssignSplitStatus assignSplitStatus = assignSplits();
        if (NO_MORE_SPLITS.equals(assignSplitStatus)) {
            LOG.info("No more splits available for subtasks");
            handleNoMoreSplits(subtaskId);
        }
    }

    public enum AssignSplitStatus {
        NO_MORE_SPLITS,
        NO_MORE_READERS;
    }
}
