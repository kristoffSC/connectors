package io.delta.flink.source;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;

import io.delta.flink.source.AddFileEnumerator.SplitFilter;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;
import org.apache.flink.core.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;

public class BoundedDeltaSourceSplitEnumerator
    implements SplitEnumerator<DeltaSourceSplit, DeltaEnumeratorStateCheckpoint<DeltaSourceSplit>> {

    private static final Logger LOG =
        LoggerFactory.getLogger(BoundedDeltaSourceSplitEnumerator.class);
    private static final int NO_SNAPSHOT_VERSION = -1;

    private final Path deltaTablePath;
    private final AddFileEnumerator<DeltaSourceSplit> fileEnumerator;
    private final FileSplitAssigner splitAssigner;
    private final long initialSnapshotVersion;
    private final Snapshot snapshot;
    private final SplitEnumeratorContext<DeltaSourceSplit> enumContext;
    private final LinkedHashMap<Integer, String> readersAwaitingSplit;
    private final HashSet<Path> pathsAlreadyProcessed;

    public BoundedDeltaSourceSplitEnumerator(
        Path deltaTablePath, AddFileEnumerator<DeltaSourceSplit> fileEnumerator,
        FileSplitAssigner splitAssigner, Configuration configuration,
        SplitEnumeratorContext<DeltaSourceSplit> enumContext) {
        this(deltaTablePath, fileEnumerator, splitAssigner, configuration, enumContext,
            NO_SNAPSHOT_VERSION, Collections.emptySet());
    }

    public BoundedDeltaSourceSplitEnumerator(
        Path deltaTablePath, AddFileEnumerator<DeltaSourceSplit> fileEnumerator,
        FileSplitAssigner splitAssigner, Configuration configuration,
        SplitEnumeratorContext<DeltaSourceSplit> enumContext, long initialSnapshotVersion,
        Collection<Path> alreadyDiscoveredPaths) {
        this.fileEnumerator = fileEnumerator;
        this.splitAssigner = splitAssigner;
        this.enumContext = enumContext;
        this.readersAwaitingSplit = new LinkedHashMap<>();
        this.deltaTablePath = deltaTablePath;

        DeltaLog deltaLog =
            DeltaLog.forTable(configuration, deltaTablePath.toUri().normalize().toString());
        this.snapshot = (initialSnapshotVersion == NO_SNAPSHOT_VERSION) ?
            deltaLog.snapshot() : deltaLog.getSnapshotForVersionAsOf(initialSnapshotVersion);

        this.initialSnapshotVersion = snapshot.getVersion();

        // TODO Add this to Enumerator State and actually use it in start method.
        this.pathsAlreadyProcessed = new HashSet<>(alreadyDiscoveredPaths);
    }

    @Override
    public void start() {
        // TODO Initial data read. This should be done in batches since snapshot.getAllFiles()
        //  can have millions of files, and we would OOM the Job Manager
        //  if we would read all of them at once.
        try {
            // TODO check for already processed files and skipp them.
            //  This is needed for Checkpoint restoration.
            AddFileEnumeratorContext context = setUpEnumeratorContext();
            List<DeltaSourceSplit> splits = fileEnumerator
                .enumerateSplits(context, (SplitFilter<Path>) pathsAlreadyProcessed::add);
            addSplits(splits);
        } catch (Exception e) {
            // TODO Create Delta Source Exception
            throw new RuntimeException(e);
        }
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
    public DeltaEnumeratorStateCheckpoint<DeltaSourceSplit> snapshotState() throws Exception {

        // The Flink's SplitAssigner interface uses FileSourceSplit
        // in its signatures and return types even though it is expected to be extended
        // by other implementations. This "trick" is used also in Flink source code
        // by bundled Hive connector -
        // https://github.com/apache/flink/blob/release-1.14/flink-connectors/flink-connector-hive/src/main/java/org/apache/flink/connectors/hive/ContinuousHiveSplitEnumerator.java#L137
        @SuppressWarnings("unchecked")
        Collection<DeltaSourceSplit> remainingSplits =
            (Collection<DeltaSourceSplit>) (Collection<?>) splitAssigner.remainingSplits();

        return DeltaEnumeratorStateCheckpoint.fromCollectionSnapshot(
            deltaTablePath, initialSnapshotVersion, remainingSplits, pathsAlreadyProcessed);
    }

    @Override
    public void close() throws IOException {
        // no resources to close
    }

    private void addSplits(List<DeltaSourceSplit> splits) {
        // We are creating new Array to trick Java type check since the signature of this
        // constructor is "? extends E". The downside is that this constructor creates an extra
        // array and copies all elements which is not efficient.
        // However, there is no point for construction our custom Interface and Implementation
        // for splitAssigner just to have needed type.
        splitAssigner.addSplits(new ArrayList<>(splits));
    }

    private void assignSplits(int subtaskId) {
        final Iterator<Map.Entry<Integer, String>> awaitingReader =
            readersAwaitingSplit.entrySet().iterator();

        while (awaitingReader.hasNext()) {
            Map.Entry<Integer, String> nextAwaiting = awaitingReader.next();

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
                // TODO for batch load we will need to modify this to get a new batch from Delta.;
                LOG.info("No more splits available for subtasks");
                enumContext.signalNoMoreSplits(subtaskId);
                LOG.info("No more splits available for subtask {}", subtaskId);
                break;
            }
        }
    }

    private AddFileEnumeratorContext setUpEnumeratorContext() {
        String pathString = SourceUtils.pathToString(deltaTablePath);
        return new AddFileEnumeratorContext(pathString, snapshot.getAllFiles());
    }
}
