package io.delta.flink.source.internal.builder;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class Validator {

    private final Set<String> validationMessages = new HashSet<>();

    public Validator checkNotNull(Object reference, String errorMessage) {
        if (reference == null) {
            validationMessages.add(String.valueOf(errorMessage));
        }
        return this;
    }

    public Validator checkArgument(boolean condition, String errorMessage) {
        if (!condition) {
            validationMessages.add(String.valueOf(errorMessage));
        }
        return this;
    }

    public Set<String> getValidationMessages() {
        return Collections.unmodifiableSet(validationMessages);
    }

    public boolean containsMessages() {
        return !this.validationMessages.isEmpty();
    }
}
