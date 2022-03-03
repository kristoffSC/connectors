package io.delta.flink.source.internal.exceptions;

import java.io.IOException;

import io.delta.flink.source.internal.file.AddFileEnumeratorContext;
import io.delta.flink.source.internal.utils.SourceUtils;
import org.apache.flink.core.fs.Path;

/**
 * The utility class that provides a factory methods for various cases where {@link
 * DeltaSourceException} has to be thrown.
 */
public final class DeltaSourceExceptions {

    private static final String EMPTY_STRUNG = "";

    private DeltaSourceExceptions() {

    }

    /**
     * Wraps given {@link Throwable} with {@link DeltaSourceException}. The returned exception
     * object will use {@link Throwable#toString()} on provided {@code Throwable} to get its
     * exception message.
     *
     * @param tablePath       Path to Delta Table for which this exception occurred.
     * @param snapshotVersion Delta Table Snapshot version for which this exception occurred.
     * @param t               {@link Throwable} that should be wrapped with {@link
     *                        DeltaSourceException}
     * @return {@link DeltaSourceException} wrapping original {@link Throwable}
     */
    public static DeltaSourceException generalSourceException(String tablePath, String filePath,
        long snapshotVersion, Throwable t) {
        return new DeltaSourceException(tablePath, filePath, snapshotVersion, t);
    }

    /**
     * Creates new {@link DeltaSourceException} object that can be used for {@link IOException}
     * thrown from {@link io.delta.flink.source.internal.file.AddFileEnumerator#enumerateSplits(
     *AddFileEnumeratorContext, io.delta.flink.source.internal.file.AddFileEnumerator.SplitFilter)}
     * <p>
     * <p>
     * Wraps given {@link Throwable} with {@link DeltaSourceException}. The returned exception
     * object will use defined error message for this case.
     *
     * @param context  The {@link AddFileEnumeratorContext} for which this exception occurred.
     * @param filePath The {@link Path} for Parquet file that caused this exception.
     * @param e        Wrapped {@link IOException}
     * @return {@link DeltaSourceException} wrapping original {@code IOException}
     */
    public static DeltaSourceException fileEnumerationException(AddFileEnumeratorContext context,
        Path filePath, IOException e) {
        String stringFilePath = SourceUtils.pathToString(filePath);
        return new DeltaSourceException(context.getTablePath(), stringFilePath,
            context.getSnapshotVersion(),
            String.format("An Exception while processing Parquet Files for path %s and version %d",
                stringFilePath, context.getSnapshotVersion()), e);
    }

    public static DeltaSourceException tableMonitorException(String deltaTablePath,
        Throwable error) {
        if (error instanceof DeltaSourceException) {
            return (DeltaSourceException) error;
        }

        return new DeltaSourceException(deltaTablePath,
            String.format("Exception during monitoring Delta Table [%s] for changes",
                deltaTablePath), error);
    }

    public static void deltaSourceIgnoreChangesException(String deltaTablePath,
        long snapshotVersion) {

        throw new DeltaSourceException(deltaTablePath,
            String.format("Detected a data update in the source table at version "
                + "%d. This is currently not supported. If you'd like to ignore updates, set "
                + "the option 'ignoreChanges' to 'true'. If you would like the data update to "
                + "be reflected, please restart this query with a fresh Delta checkpoint "
                + "directory.", snapshotVersion), snapshotVersion);
    }

    public static void deltaSourceIgnoreDeleteException(String deltaTablePath,
        long snapshotVersion) {
        throw new DeltaSourceException(deltaTablePath,
            String.format("Detected deleted data (for example $removedFile) from streaming source "
                + "at version %d. This is currently not supported. If you'd like to ignore deletes "
                + "set the option 'ignoreDeletes' to 'true'.", snapshotVersion), snapshotVersion);
    }

    // Add other methods in future PRs.
}
