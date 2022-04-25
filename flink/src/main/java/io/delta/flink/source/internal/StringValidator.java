package io.delta.flink.source.internal;

import org.apache.flink.util.StringUtils;

public class StringValidator implements OptionValidator {

    @Override
    public boolean validate(String value) {
        return !StringUtils.isNullOrWhitespaceOnly(value);
    }

    @Override
    public boolean validate(boolean value) {
        return false;
    }

    @Override
    public boolean validate(int value) {
        return false;
    }

    @Override
    public boolean validate(long value) {
        return false;
    }
}
