package io.delta.flink.source.internal.exceptions;

/**
 * A runtime exception throw by {@link io.delta.flink.source.DeltaSource} components.
 */
public class DeltaSourceException extends RuntimeException {

    public DeltaSourceException(Throwable cause) {
        super(cause);
    }

    public DeltaSourceException(String message) {
        super(message);
    }

    public DeltaSourceException(String message, Throwable cause) {
        super(message, cause);
    }
}
