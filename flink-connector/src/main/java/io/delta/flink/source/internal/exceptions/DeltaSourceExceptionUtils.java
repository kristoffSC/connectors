package io.delta.flink.source.internal.exceptions;

public final class DeltaSourceExceptionUtils {

    private DeltaSourceExceptionUtils() {

    }

    public static DeltaSourceException generalSourceException(Throwable t) {
        throw new DeltaSourceException(t);
    }

    // Add other methods in future PRs.
}
