package io.delta.flink.source.internal;

public class StartingVersionValidator implements OptionValidator {

    private final NumberValidator numberValidator = new NumberValidator();

    @Override
    public boolean validate(String value) {
        if (DeltaSourceOptions.STARTING_VERSION_LATEST.equals(value)) {
            return true;
        } else {
            return numberValidator.validate(value);
        }
    }

    @Override
    public boolean validate(boolean value) {
        return false;
    }

    @Override
    public boolean validate(int value) {
        return numberValidator.validate(value);
    }

    @Override
    public boolean validate(long value) {
        return numberValidator.validate(value);
    }
}
