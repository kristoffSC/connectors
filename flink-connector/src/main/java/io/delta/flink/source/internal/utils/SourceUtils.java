package io.delta.flink.source.internal.utils;

import org.apache.flink.core.fs.Path;

/**
 * A utility class for Source connector
 */
public final class SourceUtils {

    private SourceUtils() {

    }

    /**
     * Converts Flink's {@link Path} to String
     * @param path Flink's {@link Path}
     * @return String representation of {@link Path}
     */
    public static String pathToString(Path path) {
        return path.toUri().normalize().toString();
    }
}
