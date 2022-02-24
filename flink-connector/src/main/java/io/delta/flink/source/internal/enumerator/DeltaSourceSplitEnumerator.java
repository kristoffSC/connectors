package io.delta.flink.source.internal.enumerator;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import javax.annotation.Nullable;

import io.delta.flink.source.internal.DeltaSourceConfiguration;
import io.delta.flink.source.internal.file.AddFileEnumeratorContext;
import io.delta.flink.source.internal.state.DeltaEnumeratorStateCheckpoint;
import io.delta.flink.source.internal.state.DeltaSourceSplit;
import io.delta.flink.source.internal.utils.SourceUtils;
import io.delta.flink.source.internal.utils.TransitiveOptional;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;
import org.apache.flink.core.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static io.delta.flink.source.internal.enumerator.DeltaSourceSplitEnumerator.AssignSplitStatus.NO_MORE_READERS;
import static io.delta.flink.source.internal.enumerator.DeltaSourceSplitEnumerator.AssignSplitStatus.NO_MORE_SPLITS;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;
import io.delta.standalone.actions.AddFile;

/**
 * A base class for {@link SplitEnumerator} used by {@link io.delta.flink.source.DeltaSource}
 * <p>
 * The implementations that will choose to extend this class will have to implement two abstract
 * methods:
 * <ul>
 *  <li>{@link DeltaSourceSplitEnumerator#getInitialSnapshot(long)}</li>
 *  <li>{@link DeltaSourceSplitEnumerator#handleNoMoreSplits(int)}</li>
 * </ul>
 */
public abstract class DeltaSourceSplitEnumerator implements
    SplitEnumerator<DeltaSourceSplit, DeltaEnumeratorStateCheckpoint<DeltaSourceSplit>> {

    /**
     * Constant used when no snapshot value was specified, for example first Source initialization
     * without any previously taken snapshot.
     */
    protected static final int NO_SNAPSHOT_VERSION = -1;
    private static final Logger LOG =
        LoggerFactory.getLogger(DeltaSourceSplitEnumerator.class);
    /**
     * Path to Delta Table from which this {@code DeltaSource} should read.
     */
    protected final Path deltaTablePath;

    /**
     * A {@link FileSplitAssigner} that should be used by this {@code SourceEnumerator}.
     */
    protected final FileSplitAssigner splitAssigner;

    /**
     * The Delta {@link Snapshot} that we should read data from.
     */
    protected final Snapshot snapshot;

    /**
     * A {@link SplitEnumeratorContext} assigned to this {@code SourceEnumerator}.
     */
    protected final SplitEnumeratorContext<DeltaSourceSplit> enumContext;

    /**
     * Map containing all readers that have requested the split.
     */
    protected final LinkedHashMap<Integer, String> readersAwaitingSplit;

    /**
     * Set with already processed paths for Parquet Files. This map is used during recovery from
     * checkpoint.
     */
    protected final HashSet<Path> pathsAlreadyProcessed;

    /**
     * A {@link DeltaLog} instance to read snapshot data and get table changes from.
     */
    protected final DeltaLog deltaLog;

    /**
     * An initial {@link Snapshot} version that we start reading Delta Table from.
     */
    protected final long initialSnapshotVersion;

    /**
     * A {@link DeltaSourceConfiguration} used while creating
     * {@link io.delta.flink.source.DeltaSource}
     */
    protected final DeltaSourceConfiguration sourceConfiguration;

    protected DeltaSourceSplitEnumerator(
        Path deltaTablePath, FileSplitAssigner splitAssigner, Configuration configuration,
        SplitEnumeratorContext<DeltaSourceSplit> enumContext,
        DeltaSourceConfiguration sourceConfiguration,
        long checkpointSnapshotVersion, Collection<Path> alreadyDiscoveredPaths) {

        this.splitAssigner = splitAssigner;
        this.enumContext = enumContext;
        this.readersAwaitingSplit = new LinkedHashMap<>();
        this.deltaTablePath = deltaTablePath;
        this.sourceConfiguration = sourceConfiguration;

        this.deltaLog =
            DeltaLog.forTable(configuration, SourceUtils.pathToString(deltaTablePath));
        this.snapshot = getInitialSnapshot(checkpointSnapshotVersion);

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
            String hostInfo =
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

    /**
     * The implementation of this method should handle case, where there is no more splits that
     * could be assigned to Source Readers.
     * <p>
     * This method is called by {@link DeltaSourceSplitEnumerator#handleSplitRequest(int, String)}
     * method.
     *
     * @param subtaskId the subtask id of the source reader who sent the source spit request event.
     */
    protected abstract void handleNoMoreSplits(int subtaskId);

    /**
     * The implementation of this method encapsulates the initial snapshot creation logic.
     * <p>
     * This method is called from {@code DeltaSourceSplitEnumerator} constructor during object
     * initialization.
     *
     * @param checkpointSnapshotVersion - version of snapshot from checkpoint. If the value is equal
     *                                  to {@link #NO_SNAPSHOT_VERSION} it means that this is first
     *                                  Source initialization and not a recovery from a Flink's
     *                                  checkpoint.
     * @return A {@link Snapshot} that will be used as an initial Delta Table {@code Snapshot} to
     * read data from.
     */
    protected abstract Snapshot getInitialSnapshot(long checkpointSnapshotVersion);

    @SuppressWarnings("unchecked")
    protected Collection<DeltaSourceSplit> getRemainingSplits() {
        // The Flink's SplitAssigner interface uses FileSourceSplit
        // in its signatures.
        // This "trick" is also used in Flink source code by bundled Hive connector -
        // https://github.com/apache/flink/blob/release-1.14/flink-connectors/flink-connector-hive/src/main/java/org/apache/flink/connectors/hive/ContinuousHiveSplitEnumerator.java#L137
        return (Collection<DeltaSourceSplit>) (Collection<?>) splitAssigner.remainingSplits();
    }

    @SuppressWarnings("unchecked")
    protected void addSplits(List<DeltaSourceSplit> splits) {
        // We are doing this double cast trick here because  Flink's SplitAssigner interface uses
        // FileSourceSplit in its signatures instead something like <? extends FileSourceSplit>
        // There is no point for construction our custom Interface and Implementation
        // for splitAssigner just to have needed type.
        splitAssigner.addSplits((Collection<FileSourceSplit>) (Collection<?>) splits);
    }

    protected AddFileEnumeratorContext setUpEnumeratorContext(List<AddFile> addFiles,
        long snapshotVersion) {
        String pathString = SourceUtils.pathToString(deltaTablePath);
        return new AddFileEnumeratorContext(pathString, addFiles, snapshotVersion);
    }

    private AssignSplitStatus assignSplits() {
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

    protected TransitiveOptional<Snapshot> getSnapshotFromCheckpoint(
        long checkpointSnapshotVersion) {
        if (checkpointSnapshotVersion != NO_SNAPSHOT_VERSION) {
            return TransitiveOptional.ofNullable(
                deltaLog.getSnapshotForVersionAsOf(checkpointSnapshotVersion));
        }
        return TransitiveOptional.empty();
    }

    protected TransitiveOptional<Snapshot> getHeadSnapshot() {
        return TransitiveOptional.ofNullable(deltaLog.snapshot());
    }

    private void assignSplits(int subtaskId) {
        AssignSplitStatus assignSplitStatus = assignSplits();
        if (NO_MORE_SPLITS.equals(assignSplitStatus)) {
            LOG.info("No more splits available for subtasks");
            handleNoMoreSplits(subtaskId);
        }
    }

    @VisibleForTesting
    Snapshot getSnapshot() {
        return this.snapshot;
    }

    public enum AssignSplitStatus {
        NO_MORE_SPLITS,
        NO_MORE_READERS
    }
}
