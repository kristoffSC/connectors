package io.delta.flink.utils;

import java.util.Objects;

public class ExecutionExpectedResult {

    private final int expectedNumberOfRecords;

    private final long expectedVersion;

    private final int expectedNumberOfFiles;

    public ExecutionExpectedResult(
            int expectedNumberOfRecords,
            long expectedVersion,
            int expectedNumberOfFiles) {
        this.expectedNumberOfRecords = expectedNumberOfRecords;
        this.expectedVersion = expectedVersion;
        this.expectedNumberOfFiles = expectedNumberOfFiles;
    }

    public int getExpectedNumberOfRecords() {
        return expectedNumberOfRecords;
    }

    public long getExpectedVersion() {
        return expectedVersion;
    }

    public int getExpectedNumberOfFiles() {
        return expectedNumberOfFiles;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ExecutionExpectedResult that = (ExecutionExpectedResult) o;
        return getExpectedNumberOfRecords() == that.getExpectedNumberOfRecords()
            && getExpectedVersion() == that.getExpectedVersion()
            && getExpectedNumberOfFiles() == that.getExpectedNumberOfFiles();
    }

    @Override
    public int hashCode() {
        return Objects.hash(getExpectedNumberOfRecords(), getExpectedVersion(),
            getExpectedNumberOfFiles());
    }

    @Override
    public String toString() {
        return "ExecutionExpectedResult{" +
            "expectedNumberOfRecords=" + expectedNumberOfRecords +
            ", expectedVersion=" + expectedVersion +
            ", expectedNumberOfLines=" + expectedNumberOfFiles +
            '}';
    }
}
