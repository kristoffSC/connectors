package io.delta.flink.source;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import javax.annotation.Nullable;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;
import org.apache.flink.core.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.actions.AddFile;

public class BoundedDeltaSourceFileEnumerator
    implements SplitEnumerator<DeltaSourceSplit, DeltaPendingSplitsCheckpoint<DeltaSourceSplit>> {

    private static final Logger LOG =
        LoggerFactory.getLogger(BoundedDeltaSourceFileEnumerator.class);

    private final Path deltaTablePath;
    private final AddFileEnumerator<DeltaSourceSplit> fileEnumerator;
    private final FileSplitAssigner splitAssigner;
    private final Configuration configuration;
    private final long initialSnapshotVersion;
    private final SplitEnumeratorContext<DeltaSourceSplit> enumContext;
    private final LinkedHashMap<Integer, String> readersAwaitingSplit;

    public BoundedDeltaSourceFileEnumerator(
        Path deltaTablePath, AddFileEnumerator<DeltaSourceSplit> fileEnumerator,
        FileSplitAssigner splitAssigner, SplitEnumeratorContext<DeltaSourceSplit> enumContext) {
        this(deltaTablePath, fileEnumerator, splitAssigner, enumContext, -1);
    }

    public BoundedDeltaSourceFileEnumerator(
        Path deltaTablePath, AddFileEnumerator<DeltaSourceSplit> fileEnumerator,
        FileSplitAssigner splitAssigner, SplitEnumeratorContext<DeltaSourceSplit> enumContext,
        long initialSnapshotVersion) {
        this.deltaTablePath = deltaTablePath;
        this.fileEnumerator = fileEnumerator;
        this.splitAssigner = splitAssigner;
        this.enumContext = enumContext;
        this.initialSnapshotVersion = initialSnapshotVersion;
        this.readersAwaitingSplit = new LinkedHashMap<>();

        Configuration conf = new Configuration();
        conf.set("parquet.compression", "SNAPPY");
        this.configuration = conf;
    }

    @Override
    public void start() {
        // TODO Initial data read. This should be done in batches since snapshot.getAllFiles()
        //  can have millions of files, and we would OOM the Job Manager
        //  if we would read all of them at once.
        try {
            // TODO Since this is the Bounded mode,
            //  we must use initialSnapshotVersion and deltaLog.getSnapshotForVersionAsOf(...)
            //  to make sure that we are reading the same table after recovery
            String deltaTableStringPath = deltaTablePath.toUri().normalize().toString();
            DeltaLog deltaLog = DeltaLog.forTable(configuration, deltaTableStringPath);
            List<AddFile> allFiles = deltaLog.snapshot().getAllFiles();

            AddFileEnumeratorContext context = new AddFileEnumeratorContext(
                deltaTableStringPath, allFiles);
            List<DeltaSourceSplit> splits = fileEnumerator.enumerateSplits(context);
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
    public DeltaPendingSplitsCheckpoint<DeltaSourceSplit> snapshotState() throws Exception {

        // The Flink's SplitAssigner interface uses FileSourceSplit
        // in its signatures and return types even though it is expected to be extended
        // by other implementations. This "trick" is used also in Flink source code
        // by bundled Hive connector -
        // https://github.com/apache/flink/blob/release-1.14/flink-connectors/flink-connector-hive/src/main/java/org/apache/flink/connectors/hive/ContinuousHiveSplitEnumerator.java#L137
        @SuppressWarnings("unchecked")
        Collection<DeltaSourceSplit> remainingSplits =
            (Collection<DeltaSourceSplit>) (Collection<?>) splitAssigner.remainingSplits();

        return DeltaPendingSplitsCheckpoint.fromCollectionSnapshot(initialSnapshotVersion,
            remainingSplits);
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
        Iterator<Entry<Integer, String>> awaitingReader =
            readersAwaitingSplit.entrySet().iterator();
        while (awaitingReader.hasNext()) {
            Map.Entry<Integer, String> nextAwaiting = awaitingReader.next();
            String hostname = nextAwaiting.getValue();
            int awaitingSubtask = nextAwaiting.getKey();
            Optional<FileSourceSplit> nextSplit = splitAssigner.getNext(hostname);
            if (nextSplit.isPresent()) {
                enumContext.assignSplit((DeltaSourceSplit) nextSplit.get(), awaitingSubtask);
                awaitingReader.remove();
            } else {
                // TODO for batch load we will need to modify this to get a new batch from Delta.
                enumContext.signalNoMoreSplits(subtaskId);
                LOG.info("No more splits available for subtask {}", subtaskId);
            }
        }
    }
}
