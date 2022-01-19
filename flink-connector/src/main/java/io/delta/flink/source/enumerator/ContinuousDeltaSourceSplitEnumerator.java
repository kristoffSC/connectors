package io.delta.flink.source.enumerator;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import io.delta.flink.source.DeltaSourceException;
import io.delta.flink.source.DeltaSourceSplitEnumerator;
import io.delta.flink.source.file.AddFileEnumerator;
import io.delta.flink.source.file.AddFileEnumerator.SplitFilter;
import io.delta.flink.source.file.AddFileEnumeratorContext;
import io.delta.flink.source.state.DeltaSourceSplit;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;
import org.apache.flink.core.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.delta.standalone.actions.AddFile;

public class ContinuousDeltaSourceSplitEnumerator extends DeltaSourceSplitEnumerator {

    private static final Logger LOG =
        LoggerFactory.getLogger(BoundedDeltaSourceSplitEnumerator.class);

    private final AddFileEnumerator<DeltaSourceSplit> fileEnumerator;

    private final TableMonitor tableMonitor;

    public ContinuousDeltaSourceSplitEnumerator(
        Path deltaTablePath, AddFileEnumerator<DeltaSourceSplit> fileEnumerator,
        FileSplitAssigner splitAssigner, Configuration configuration,
        SplitEnumeratorContext<DeltaSourceSplit> enumContext) {
        this(deltaTablePath, fileEnumerator, splitAssigner, configuration, enumContext,
            NO_SNAPSHOT_VERSION, Collections.emptySet());
    }

    public ContinuousDeltaSourceSplitEnumerator(
        Path deltaTablePath, AddFileEnumerator<DeltaSourceSplit> fileEnumerator,
        FileSplitAssigner splitAssigner, Configuration configuration,
        SplitEnumeratorContext<DeltaSourceSplit> enumContext, long initialSnapshotVersion,
        Collection<Path> alreadyDiscoveredPaths) {

        super(deltaTablePath, splitAssigner, configuration, enumContext, initialSnapshotVersion,
            alreadyDiscoveredPaths);

        this.fileEnumerator = fileEnumerator;

        // Maybe we could inject it from the provider.
        this.tableMonitor = new TableMonitor(deltaLog, this.initialSnapshotVersion);
    }

    @Override
    public void start() {

        // TODO pass continuousEnumerationSettings to
        //  DEFAULT_CONTINUOUS_SPLIT_ENUMERATOR_PROVIDER and ContinuousDeltaSourceSplitEnumerator
        //  and use them in start method.

        // TODO Initial data read. This should be done in chunks since snapshot.getAllFiles()
        //  can have millions of files, and we would OOM the Job Manager
        //  if we would read all of them at once.

        //get data for start version
        try {
            LOG.info("Getting data for start version - {}", snapshot.getVersion());
            List<DeltaSourceSplit> splits = prepareSplits(snapshot.getAllFiles());
            addSplits(splits);
        } catch (Exception e) {
            throw new DeltaSourceException(e);
        }

        // TODO add tests to check split creation//assignment granularity is in scope of VersionLog.
        //monitor for changes
        enumContext.callAsync(
            tableMonitor, // executed sequentially by ScheduledPool Thread.
            this::processDiscoveredVersions, // executed by Flink's Source-Coordinator Thread.
            1000,
            5000);
    }

    @Override
    protected void handleNoMoreSplits(int subtaskId) {
        //Do nothing since readers have to continuously wait for new data.
    }

    private List<DeltaSourceSplit> prepareSplits(List<AddFile> addFiles) {
        try {
            AddFileEnumeratorContext context = setUpEnumeratorContext(addFiles);
            return fileEnumerator
                .enumerateSplits(context, (SplitFilter<Path>) pathsAlreadyProcessed::add);
        } catch (Exception e) {
            throw new DeltaSourceException("Exception wile preparing Splits.", e);
        }
    }

    private void processDiscoveredVersions(MonitorTableState monitorTableState, Throwable error) {
        if (error != null) {
            LOG.error("Failed to enumerate files", error);
            return;
        }

        List<AddFile> newAddFiles = monitorTableState.getResult();
        this.initialSnapshotVersion = monitorTableState.getSnapshotVersion();

        List<DeltaSourceSplit> discoveredSplits = prepareSplits(newAddFiles);
        addSplits(discoveredSplits);

        // TODO this substskId makes no sense here. Refactor This.
        assignSplits(-1);

    }
}
