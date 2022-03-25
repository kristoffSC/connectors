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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.delta.standalone.Snapshot;
import io.delta.standalone.actions.AddFile;

/**
 * This implementation of {@link TableProcessor} process only Delta table changes starting from
 * specified {@link io.delta.standalone.Snapshot} version. This implementation does not read {@code
 * Snapshot} content.
 *
 * <p>
 * The {@code Snapshot} version is specified by {@link TableMonitor} used when creating an
 * instance of {@code ChangesProcessor}.
 */
public class ChangesProcessor implements ContinuousTableProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(ChangesProcessor.class);

    /**
     * The {@link TableMonitor} instance used to monitor Delta table for changes.
     */
    // TODO PR 7 Will be used for monitoring for changes.
    private final TableMonitor tableMonitor;

    /**
     * A {@link SplitEnumeratorContext} used for this {@code ChangesProcessor}.
     */
    private final SplitEnumeratorContext<DeltaSourceSplit> enumContext;

    /**
     * Set with already processed paths for Parquet Files. Processor will skip not process parquet
     * files from this set.
     * <p>
     * The use case for this set is a recovery from checkpoint scenario, where we don't want to
     * reprocess already processed Parquet files.
     */
    // TODO PR 7 - update this set in prepareSplits method.
    private final HashSet<Path> alreadyProcessedPaths;

    // TODO PR 7 Will be used for monitoring for changes.
    // private final boolean ignoreChanges;

    // TODO PR 7 Will be used for monitoring for changes.
    // private final boolean ignoreDeletes;

    /**
     * A Delta table {@link io.delta.standalone.Snapshot} version currently used by this {@link
     * ChangesProcessor} to read changes from. This value will be updated after discovering new
     * versions on Delta table.
     */
    // TODO PR 7 version will be updated by processDiscoveredVersions method after discovering new
    //  changes.
    private long currentSnapshotVersion;

    public ChangesProcessor(TableMonitor tableMonitor,
        SplitEnumeratorContext<DeltaSourceSplit> enumContext,
        Collection<Path> alreadyProcessedPaths) {
        this.tableMonitor = tableMonitor;
        this.enumContext = enumContext;
        this.alreadyProcessedPaths = new HashSet<>(alreadyProcessedPaths);
        this.currentSnapshotVersion = this.tableMonitor.getMonitorVersion();
    }

    /**
     * Starts processing changes that were added to Delta table starting from version specified by
     * {@link #currentSnapshotVersion} field by converting them to {@link DeltaSourceSplit} objects.
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

    /**
     * @return Collection of {@link Path} objects that corresponds to Parquet files processed by
     * this processor in scope of {@link #getSnapshotVersion()}.
     */
    @Override
    public Collection<Path> getAlreadyProcessedPaths() {
        return alreadyProcessedPaths;
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
        List<ChangesPerVersion> newActions = monitorTableResult.getChanges();

        newActions.stream()
            .map(this::processActions)
            .map(this::prepareSplits)
            .forEachOrdered(processCallback);
    }

    // TODO PR 7 Add implementation and tests
    private List<AddFile> processActions(ChangesPerVersion changes) {
        return Collections.emptyList();
    }

    // TODO PR 7 Add implementation and tests
    private List<DeltaSourceSplit> prepareSplits(List<AddFile> addFiles) {
        return Collections.emptyList();
    }
}
