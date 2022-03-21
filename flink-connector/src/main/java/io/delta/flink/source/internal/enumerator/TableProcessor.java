package io.delta.flink.source.internal.enumerator;

import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

import io.delta.flink.source.internal.state.DeltaSourceSplit;
import org.apache.flink.core.fs.Path;

/**
 * A processor for Delta table data.
 * <p>
 * The implementations of this interface should encapsulate logic for processing Delta table Changes
 * and Add Files.
 */
public interface TableProcessor {

    /**
     * Process Delta table data. Can call {@code processCallback} during this process.
     *
     * @param processCallback A {@link Consumer} callback that can be called during Delta table
     *                        processing. The exact condition when this callback will be called
     *                        depends on {@code TableProcessor} implementation.
     */
    void process(Consumer<List<DeltaSourceSplit>> processCallback);

    /**
     * @return A {@link io.delta.standalone.Snapshot} version on which this processor operates.
     */
    long getSnapshotVersion();

    /**
     * @return A {@link Collection} of already processed Parquet file path that this processor
     * processed.
     * <p>
     * The exact scope of this collection depends on the implementation. Some may return every path
     * that was processed (for example {@link SnapshotProcessor}) and others can return only those
     * that were processed in scope of given snapshot version (for example {@link
     * SnapshotAndChangesTableProcessor});
     */
    Collection<Path> getAlreadyProcessedPaths();
}
