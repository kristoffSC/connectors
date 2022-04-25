package io.delta.flink.source.internal.builder;

import java.util.regex.Pattern;

import org.apache.flink.util.StringUtils;

public class NumberValidator implements OptionValidator{

    /**
     * RegExp pattern for zero or positive integers only.
     */
    private final Pattern pattern = Pattern.compile("^0$|^[1-9]\\d*$");

    @Override
    public boolean validate(String value) {
        return !StringUtils.isNullOrWhitespaceOnly(value) && pattern.matcher(value).matches();
    }

    @Override
    public boolean validate(boolean value) {
        return false;
    }

    @Override
    public boolean validate(int value) {
        return value >= 0;
    }

    @Override
    public boolean validate(long value) {
        return value >= 0;
    }
}
