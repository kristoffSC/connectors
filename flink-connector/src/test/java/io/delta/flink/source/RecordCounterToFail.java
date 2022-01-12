package io.delta.flink.source;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import org.apache.flink.streaming.api.datastream.DataStream;

public class RecordCounterToFail implements Serializable {

    private static AtomicInteger records;
    private static CompletableFuture<Void> fail;
    private static CompletableFuture<Void> continueProcessing;

    public static <T> DataStream<T> wrapWithFailureAfter(DataStream<T> stream,
        FailCheck failCheck) {

        records = new AtomicInteger();
        fail = new CompletableFuture<>();
        continueProcessing = new CompletableFuture<>();

        return stream.map(
            record -> {
                boolean notFailedYet = !fail.isDone();
                int processedCount = records.incrementAndGet();
                if (notFailedYet && failCheck.test(processedCount)) {
                    fail.complete(null);
                    continueProcessing.get();
                }
                return record;
            });
    }

    public static void waitToFail() throws Exception {
        int retryMax = 10;
        int retry = 0;
        while (retry++ < retryMax) {
            try {
                fail.get(1, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                // Hush
            }
        }
    }

    public static void continueProcessing() {
        continueProcessing.complete(null);
    }

    // We need to extend Serializable interface to allow Flink serialize Lambda expression.
    // Alternative would be adding (Predicate<Integer> & Serializable) cast to method call,
    // which does not look good.
    @FunctionalInterface
    public interface FailCheck extends Predicate<Integer>, Serializable {

    }
}
