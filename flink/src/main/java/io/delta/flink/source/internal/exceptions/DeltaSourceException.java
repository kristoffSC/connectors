package io.delta.flink.source.internal.exceptions;

/**
 * A runtime exception throw by {@link io.delta.flink.source.DeltaSource} components.
 */
public class DeltaSourceException extends RuntimeException {

    private final String tablePath;

    private final long snapshotVersion;

    public DeltaSourceException(String tablePath, long snapshotVersion, Throwable cause) {
        super(cause);
        this.tablePath = tablePath;
        this.snapshotVersion = snapshotVersion;
    }

    public DeltaSourceException(String tablePath, long snapshotVersion, String message) {
        super(message);
        this.tablePath = tablePath;
        this.snapshotVersion = snapshotVersion;
    }

    public DeltaSourceException(String tablePath, long snapshotVersion, String message,
        Throwable cause) {
        super(message, cause);
        this.tablePath = tablePath;
        this.snapshotVersion = snapshotVersion;
    }

    public String getTablePath() {
        return tablePath;
    }

    public long getSnapshotVersion() {
        return snapshotVersion;
    }
}
