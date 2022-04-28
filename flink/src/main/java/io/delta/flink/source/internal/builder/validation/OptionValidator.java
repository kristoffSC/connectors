package io.delta.flink.source.internal.builder.validation;

public interface OptionValidator {

    boolean validate(String value);

    boolean validate(boolean value);

    boolean validate(int value);

    boolean validate(long value);

    class DefaultValidator implements OptionValidator {

        @Override
        public boolean validate(String value) {
            return true;
        }

        @Override
        public boolean validate(boolean value) {
            return true;
        }

        @Override
        public boolean validate(int value) {
            return true;
        }

        @Override
        public boolean validate(long value) {
            return true;
        }
    }
}
