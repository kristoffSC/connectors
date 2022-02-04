package io.delta.flink.source.internal.exceptions;

public final class DeltaSourceExceptionUtils {

    private DeltaSourceExceptionUtils() {

    }

    public static DeltaSourceException generalSourceException(Throwable t) {
        throw new DeltaSourceException(t);
    }

    public static DeltaSourceException generalSourceException(String message, Throwable t) {
        throw new DeltaSourceException(message, t);
    }

    public static void deltaSourceIgnoreChangesException(long snapshotVersion) {

        throw new DeltaSourceException(
            String.format("Detected a data update in the source table at version "
                + "%d. This is currently not supported. If you'd like to ignore updates, set "
                + "the option 'ignoreChanges' to 'true'. If you would like the data update to "
                + "be reflected, please restart this query with a fresh Delta checkpoint "
                + "directory.", snapshotVersion));
    }

    public static void deltaSourceIgnoreDeleteException(long snapshotVersion) {
        throw new DeltaSourceException(
            String.format("Detected deleted data (for example $removedFile) from streaming source "
                + "at version %d. This is currently not supported. If you'd like to ignore deletes "
                + "set the option 'ignoreDeletes' to 'true'.", snapshotVersion));
    }

    public static void usedMutualExcludedOptionsException(String... excludedOption) {
        throw new DeltaSourceException(
            String.format("Used Mutual Excluded options for Source definition [%s]",
                String.join(";", excludedOption)));
    }

    public static void invalidOptionNameException(String optionName) {
        throw new DeltaSourceException(
            String.format("Invalid option [%s] used for Delta Source Connector.", optionName));
    }

    public static void missingPartitionValueException(String partitionName) {
        throw new DeltaSourceException(
            String.format("Cannot find the partition value in Delta MetaData for column %s",
                partitionName));
    }

    public static void notPartitionedTableException(String columnName) {
        throw new DeltaSourceException(
            String.format(
                "Attempt to get a value for partition column from unpartitioned Delta Table. "
                    + "Column name %s", columnName));
    }
}
