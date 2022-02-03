package io.delta.flink.source.utils;


import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Supplier;

// TODO Add Tests
public class TransitiveOptional<T> {

    private static final TransitiveOptional<?> EMPTY = new TransitiveOptional<>();

    private final T decorated;

    private TransitiveOptional() {
        this.decorated = null;
    }

    private TransitiveOptional(T value) {
        Objects.requireNonNull(value);
        this.decorated = value;
    }

    public static <T> TransitiveOptional<T> of(T value) {
        return new TransitiveOptional<>(value);
    }

    public static <T> TransitiveOptional<T> ofNullable(T value) {
        return value == null ? empty() : of(value);
    }

    public static <T> TransitiveOptional<T> empty() {
        @SuppressWarnings("unchecked")
        TransitiveOptional<T> t = (TransitiveOptional<T>) EMPTY;
        return t;
    }

    public T get() {
        if (decorated == null) {
            throw new NoSuchElementException("No value present");
        }
        return decorated;
    }

    public TransitiveOptional<T> or(Supplier<? extends TransitiveOptional<? extends T>> supplier) {
        Objects.requireNonNull(supplier);
        if (decorated != null) {
            return this;
        } else {
            @SuppressWarnings("unchecked")
            TransitiveOptional<T> r = (TransitiveOptional<T>) supplier.get();
            return Objects.requireNonNull(r);
        }
    }
}
