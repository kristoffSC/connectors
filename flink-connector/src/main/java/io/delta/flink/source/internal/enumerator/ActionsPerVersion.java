package io.delta.flink.source.internal.enumerator;

import java.util.Collections;
import java.util.List;

/**
 * A container object that represents Delta Table changes per one {@link
 * io.delta.standalone.Snapshot} version.
 */
public class ActionsPerVersion<T> {

    private final String deltaTablePath;

    /**
     * The {@link io.delta.standalone.Snapshot} version value for these changes.
     */
    private final long snapshotVersion;

    /**
     * The list of changes of type {@code T} in scope of {@link #snapshotVersion}.
     */
    private final List<T> changes;

    private ActionsPerVersion(String deltaTablePath, long snapshotVersion, List<T> changes) {
        this.deltaTablePath = deltaTablePath;
        this.snapshotVersion = snapshotVersion;
        this.changes = changes;
    }

    public static <T> ActionsPerVersion<T> of(String deltaTablePath, long snapshotVersion,
        List<T> changes) {
        return new ActionsPerVersion<>(deltaTablePath, snapshotVersion, changes);
    }

    public long getSnapshotVersion() {
        return snapshotVersion;
    }

    public List<T> getChanges() {
        return Collections.unmodifiableList(changes);
    }

    public String getDeltaTablePath() {
        return deltaTablePath;
    }

    /**
     * @return Number of changes for this version.
     */
    public int size() {
        return changes.size();
    }
}
