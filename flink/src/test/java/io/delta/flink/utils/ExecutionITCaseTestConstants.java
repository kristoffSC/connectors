package io.delta.flink.utils;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class ExecutionITCaseTestConstants {

    private ExecutionITCaseTestConstants() {

    }

    public static final List<String> NAME_COLUMN_VALUES =
        Stream.of("Jan", "Jan").collect(Collectors.toList());

    public static final Set<String> SURNAME_COLUMN_VALUES =
        Stream.of("Kowalski", "Duda").collect(Collectors.toSet());

    public static final Set<Integer> AGE_COLUMN_VALUES =
        Stream.of(1, 2).collect(Collectors.toSet());

}
