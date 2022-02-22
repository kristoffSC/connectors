package io.delta.flink.source.internal.exceptions;

import java.io.IOException;

import io.delta.flink.source.internal.file.AddFileEnumeratorContext;
import org.apache.flink.core.fs.Path;

/**
 * The utility class that provides a factory methods for various cases where {@link
 * DeltaSourceException} has to be thrown.
 */
public final class DeltaSourceExceptions {

    private DeltaSourceExceptions() {

    }

    /**
     * Wraps given {@link Throwable} and message with {@link DeltaSourceException}.
     *
     * @param tablePath       Path to Delta Table for which this exception occurred.
     * @param snapshotVersion Delta Table Snapshot version for which this exception occurred.
     * @param t               {@link Throwable} that should be wrapped with {@link
     *                        DeltaSourceException}
     * @return {@link DeltaSourceException} wrapping original {@link Throwable}
     */
    public static DeltaSourceException generalSourceException(String tablePath,
        long snapshotVersion, Throwable t) {
        return new DeltaSourceException(tablePath, snapshotVersion, t);
    }


    public static DeltaSourceException fileEnumerationException(AddFileEnumeratorContext context,
        Path filePath, IOException e) {
        return new DeltaSourceException(context.getTablePath(), context.getSnapshotVersion(),
            String.format("An Exception while processing Parquet Files for path %s and version %d",
                filePath, context.getSnapshotVersion()), e);
    }

    // Add other methods in future PRs.
}
