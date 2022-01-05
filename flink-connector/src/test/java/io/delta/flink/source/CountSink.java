package io.delta.flink.source;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class CountSink<T> extends RichSinkFunction<T> {

    public static final AtomicInteger count = new AtomicInteger();

    @Override
    public void invoke(T value, Context context) throws Exception {
        count.incrementAndGet();
    }
}
