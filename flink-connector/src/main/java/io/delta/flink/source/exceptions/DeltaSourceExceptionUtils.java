package io.delta.flink.source.exceptions;

public final class DeltaSourceExceptionUtils {

    private DeltaSourceExceptionUtils() {

    }

    public static DeltaSourceException deltaSourceIgnoreChangesError(long snapshotVersion) {

        throw new DeltaSourceException(
            String.format("Detected a data update in the source table at version "
                + "%d. This is currently not supported. If you'd like to ignore updates, set "
                + "the option 'ignoreChanges' to 'true'. If you would like the data update to "
                + "be reflected, please restart this query with a fresh Delta checkpoint "
                + "directory.", snapshotVersion));
    }

    public static DeltaSourceException deltaSourceIgnoreDeleteError(long snapshotVersion) {
        throw new DeltaSourceException(
            String.format("Detected deleted data (for example $removedFile) from streaming source "
                + "at version %d. This is currently not supported. If you'd like to ignore deletes "
                + "set the option 'ignoreDeletes' to 'true'.", snapshotVersion));
    }
}
