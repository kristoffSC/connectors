package io.delta.flink.source;

import org.apache.flink.core.fs.Path;

public final class SourceUtils {

    private SourceUtils() {

    }

    public static String pathToString(Path path) {
        return path.toUri().normalize().toString();
    }

}
