package io.delta.flink.source.internal.enumerator;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.function.Consumer;

import io.delta.flink.source.internal.enumerator.TableMonitorResult.ChangesPerVersion;
import io.delta.flink.source.internal.state.DeltaSourceSplit;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.fs.Path;

import io.delta.standalone.actions.AddFile;

public class ChangesProcessor implements ContinuousTableProcessor {

    /**
     * The {@link TableMonitor} instance used to monitor Delta Table for changes.
     */
    // TODO PR 7 Will be used in monitor for changes.
    private final TableMonitor tableMonitor;

    private final SplitEnumeratorContext<DeltaSourceSplit> enumContext;

    private final HashSet<Path> alreadyProcessedPaths;

    // TODO PR 7 Add monitor for changes.
    // private final boolean ignoreChanges;

    // TODO PR 7 Add monitor for changes.
    // private final boolean ignoreDeletes;

    // TODO PR 7 version will be updated by processDiscoveredVersions method after discovering new
    //  changes.
    private long snapshotVersion;

    private long currentSnapshotVersion;

    public ChangesProcessor(TableMonitor tableMonitor,
        SplitEnumeratorContext<DeltaSourceSplit> enumContext,
        Collection<Path> alreadyProcessedPaths) {
        this.tableMonitor = tableMonitor;
        this.enumContext = enumContext;
        this.alreadyProcessedPaths = new HashSet<>(alreadyProcessedPaths);
        this.currentSnapshotVersion = this.tableMonitor.getMonitorVersion();
    }

    @Override
    public void process(Consumer<List<DeltaSourceSplit>> processCallback) {
        // TODO PR 7 add tests to check split creation//assignment granularity is in scope of
        //  VersionLog.
        //monitor for changes
        enumContext.callAsync(
            tableMonitor, // executed sequentially by ScheduledPool Thread.
            (tableMonitorResult, throwable) -> processDiscoveredVersions(tableMonitorResult,
                processCallback, throwable), // executed by Flink's Source-Coordinator Thread.
            5000, // PR 7 Take from DeltaSourceConfiguration
            5000); // PR 7 Take from DeltaSourceConfiguration
    }

    @Override
    public long getSnapshotVersion() {
        return this.snapshotVersion;
    }

    @Override
    public Collection<Path> getAlreadyProcessedPaths() {
        return alreadyProcessedPaths;
    }

    @Override
    public boolean isMonitoringForChanges() {
        return true;
    }


    private void processDiscoveredVersions(TableMonitorResult monitorTableResult,
        Consumer<List<DeltaSourceSplit>> processCallback, Throwable error) {
        /*        if (error != null) {
            LOG.error("Failed to enumerate files", error);
            DeltaSourceExceptionUtils.generalSourceException(error);
        }*/

        this.currentSnapshotVersion = monitorTableResult.getHighestSeenVersion();
        List<ChangesPerVersion> newActions = monitorTableResult.getChanges();

        newActions.stream()
            .map(this::processActions)
            .map(this::prepareSplits)
            .forEachOrdered(processCallback);
    }

    private List<AddFile> processActions(ChangesPerVersion changes) {
        return Collections.emptyList();
    }

    private List<DeltaSourceSplit> prepareSplits(List<AddFile> addFiles) {
        return Collections.emptyList();
    }
}
