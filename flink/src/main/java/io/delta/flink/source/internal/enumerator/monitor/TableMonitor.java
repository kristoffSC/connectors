package io.delta.flink.source.internal.enumerator.monitor;

import java.util.concurrent.Callable;

import io.delta.standalone.DeltaLog;

/**
 * This class implements a logic for monitoring Delta table for changes. The logic is implemented in
 * {@link #call()} method which should be called periodically.
 *
 * @implNote This class is stateful and mutable, meaning it keep {@link
 * io.delta.standalone.Snapshot} version to check as next. This class is also NOT Thread safe. Each
 * thread calling {@link #call()} method should have its own {@code TableMonitor} instance.
 */
public class TableMonitor implements Callable<TableMonitorResult> {

    /**
     * The Delta Log/Delta table that this instance monitor for changes.
     */
    private final DeltaLog deltaLog;

    /**
     * The "maximal" duration that each subsequent call to {@link #call()} method should take. This
     * is a soft limit, which means that implementation will try to guarantee that overall call is
     * no longer that this limit. See {@link #call()} method for details.
     */
    private final long maxDurationMillis;

    /**
     * The Delta table {@link io.delta.standalone.Snapshot} version that should be used to read data
     * in next {@link #call()} method call. This value is mutable.
     */
    private long monitorVersion;

    /**
     * Creates new instance of TableMonitor class to monitor Delta table Changes.
     *
     * @param deltaLog          The {@link DeltaLog} to monitor for changes from.
     * @param monitorVersion    The initial {@link io.delta.standalone.Snapshot} version from which
     *                          this instance will monitor for changes.
     * @param maxDurationMillis The "maximal" duration that each subsequent call to {@link #call()}
     *                          method should take. This is a soft limit, which means that
     *                          implementation will try to guarantee that overall call is * no
     *                          longer that this limit. See {@link #call()} method for details.
     */
    public TableMonitor(DeltaLog deltaLog, long monitorVersion,
        long maxDurationMillis) {
        this.deltaLog = deltaLog;
        this.monitorVersion = monitorVersion;
        this.maxDurationMillis = maxDurationMillis;
    }

    /**
     * Monitor underlying Delta table for changes. The {@link TableMonitor} will try to limit
     * execution time for this method to {@link #maxDurationMillis} value. Limit check will be done
     * per each {@link io.delta.standalone.Snapshot} version that was detected. If the {@link
     * #maxDurationMillis} limit is exceeded, logic will return.
     *
     * @return {@link TableMonitorResult} object that contains list of {@link
     * io.delta.standalone.actions.Action} per version.
     */
    @Override
    public TableMonitorResult call() throws Exception {
        // TODO PR 7 Add monitor implementation and tests, for now return null.
        return null;
    }

    public long getMonitorVersion() {
        return monitorVersion;
    }
}
