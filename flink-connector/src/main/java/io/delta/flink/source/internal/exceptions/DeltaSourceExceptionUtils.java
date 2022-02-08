package io.delta.flink.source.internal.exceptions;

/**
 * A Utility class that provides a factory methods for various cases where {@link
 * DeltaSourceException} has to be thrown.
 */
public final class DeltaSourceExceptionUtils {

    private DeltaSourceExceptionUtils() {

    }

    /**
     * Wraps given {@link Throwable} with {@link DeltaSourceException}.
     *
     * @param t {@link Throwable} that should be wrapped with {@link DeltaSourceException}
     * @return {@link DeltaSourceException} wrapping original {@link Throwable}
     */
    public static DeltaSourceException generalSourceException(Throwable t) {
        throw new DeltaSourceException(t);
    }

    // Add other methods in future PRs.
}
