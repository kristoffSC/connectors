package io.delta.flink.source.internal.enumerator.processor;

import java.util.List;
import java.util.function.Consumer;

import io.delta.flink.source.internal.DeltaSourceConfiguration;
import io.delta.flink.source.internal.DeltaSourceOptions;
import io.delta.flink.source.internal.enumerator.monitor.ChangesPerVersion;
import io.delta.flink.source.internal.enumerator.monitor.TableMonitor;
import io.delta.flink.source.internal.enumerator.monitor.TableMonitorResult;
import io.delta.flink.source.internal.file.AddFileEnumerator;
import io.delta.flink.source.internal.state.DeltaEnumeratorStateCheckpointBuilder;
import io.delta.flink.source.internal.state.DeltaSourceSplit;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.delta.standalone.Snapshot;
import io.delta.standalone.actions.Action;
import io.delta.standalone.actions.AddFile;

/**
 * This implementation of {@link TableProcessor} process only Delta table changes starting from
 * specified {@link io.delta.standalone.Snapshot} version. This implementation does not read {@code
 * Snapshot} content.
 *
 * <p>
 * The {@code Snapshot} version is specified by {@link TableMonitor} used when creating an instance
 * of {@code ChangesProcessor}.
 */
public class ChangesProcessor extends BaseTableProcessor implements ContinuousTableProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(ChangesProcessor.class);

    /**
     * The {@link TableMonitor} instance used to monitor Delta table for changes.
     */
    private final TableMonitor tableMonitor;

    /**
     * A {@link SplitEnumeratorContext} used for this {@code ChangesProcessor}.
     */
    private final SplitEnumeratorContext<DeltaSourceSplit> enumContext;

    private final ActionProcessor actionProcessor;

    private final long changesInitialOffset;

    private long currentSnapshotVersion;

    private long processedFilesForCurrentVersionCount;

    private boolean ignoreOffset;

    public ChangesProcessor(
        Path deltaTablePath, TableMonitor tableMonitor,
        SplitEnumeratorContext<DeltaSourceSplit> enumContext,
        AddFileEnumerator<DeltaSourceSplit> fileEnumerator,
        DeltaSourceConfiguration sourceConfiguration, long changesInitialOffset) {
        super(deltaTablePath, fileEnumerator);
        this.tableMonitor = tableMonitor;
        this.enumContext = enumContext;
        this.currentSnapshotVersion = this.tableMonitor.getMonitorVersion();
        this.changesInitialOffset = changesInitialOffset;
        this.actionProcessor = new ActionProcessor(
            sourceConfiguration.getValue(DeltaSourceOptions.IGNORE_CHANGES),
            sourceConfiguration.getValue(DeltaSourceOptions.IGNORE_CHANGES)
                || sourceConfiguration.getValue(DeltaSourceOptions.IGNORE_DELETES));
    }

    /**
     * Starts processing changes that were added to Delta table starting from version specified by
     * {@link #currentSnapshotVersion} field by converting them to {@link DeltaSourceSplit}
     * objects.
     *
     * @param processCallback A {@link Consumer} callback that will be called after processing all
     *                        {@link io.delta.standalone.actions.Action} and converting them to
     *                        {@link DeltaSourceSplit}. This callback will be executed for every new
     *                        discovered Delta table version.
     */
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

    /**
     * @return A {@link Snapshot} version that this processor reads changes from. The method can
     * return different values for every method call, depending whether there were any changes on
     * Delta table.
     */
    @Override
    public long getSnapshotVersion() {
        return this.currentSnapshotVersion;
    }

    @Override
    public DeltaEnumeratorStateCheckpointBuilder<DeltaSourceSplit> snapshotState(
        DeltaEnumeratorStateCheckpointBuilder<DeltaSourceSplit> checkpointBuilder) {

        checkpointBuilder.withChangesOffset(
            (ignoreOffset) ? processedFilesForCurrentVersionCount : changesInitialOffset);
        checkpointBuilder.withMonitoringForChanges(isMonitoringForChanges());

        return checkpointBuilder;
    }

    /**
     * @return return always true indicating that this processor process only changes.
     */
    @Override
    public boolean isMonitoringForChanges() {
        return true;
    }

    private void processDiscoveredVersions(TableMonitorResult monitorTableResult,
        Consumer<List<DeltaSourceSplit>> processCallback, Throwable error) {
        if (error != null) {
            LOG.error("Failed to enumerate files", error);
            // TODO Add in PR 7
            //DeltaSourceExceptionUtils.generalSourceException(error);
        }

        this.currentSnapshotVersion = monitorTableResult.getHighestSeenVersion();
        monitorTableResult.getChanges()
            .forEach(changesPerVersion -> processVersion(processCallback, changesPerVersion));
    }

    private void processVersion(
        Consumer<List<DeltaSourceSplit>> processCallback,
        ChangesPerVersion<Action> changesPerVersion) {

        ChangesPerVersion<AddFile> addFilesPerVersion =
            actionProcessor.processActions(changesPerVersion);

        CounterBasedSplitFilter versionSplitFilter =
            new CounterBasedSplitFilter((ignoreOffset) ? 0L : changesInitialOffset);

        List<DeltaSourceSplit> splits = prepareSplits(addFilesPerVersion, versionSplitFilter);
        ignoreOffset = true;

        this.processedFilesForCurrentVersionCount += versionSplitFilter.getTestCounter();

        processCallback.accept(splits);
    }
}
