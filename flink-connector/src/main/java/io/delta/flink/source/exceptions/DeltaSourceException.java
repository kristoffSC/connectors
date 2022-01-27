package io.delta.flink.source.exceptions;

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
