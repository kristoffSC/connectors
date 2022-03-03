package io.delta.flink.source.internal.exceptions;

/**
 * A runtime exception throw by {@link io.delta.flink.source.DeltaSource} components.
 */
public class DeltaSourceException extends RuntimeException {

    private static final String EMPTY_STRING = "";

    private static final long NO_VERSION = -1;

    private final String tablePath;

    private final String filePath;

    private final long snapshotVersion;

    public DeltaSourceException(String tablePath, String message, long snapshotVersion) {
        super(message);
        this.tablePath = tablePath;
        this.snapshotVersion = snapshotVersion;
        this.filePath = EMPTY_STRING;
    }

    public DeltaSourceException(String tablePath, String message, Throwable cause) {
        super(message, cause);
        this.tablePath = tablePath;
        this.snapshotVersion = NO_VERSION;
        this.filePath = EMPTY_STRING;
    }

    public DeltaSourceException(String tablePath, String filePath, long snapshotVersion,
        Throwable cause) {
        super(cause);
        this.tablePath = tablePath;
        this.snapshotVersion = snapshotVersion;
        this.filePath = filePath;
    }

    public DeltaSourceException(String tablePath, String filePath, long snapshotVersion,
        String message,
        Throwable cause) {
        super(message, cause);
        this.tablePath = tablePath;
        this.snapshotVersion = snapshotVersion;
        this.filePath = filePath;
    }

    public String getTablePath() {
        return tablePath;
    }

    public String getFilePath() {
        return filePath;
    }

    public long getSnapshotVersion() {
        return snapshotVersion;
    }
}
