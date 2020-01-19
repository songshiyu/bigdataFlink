package com.song.flink.util;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @author songshiyu
 * @date 2020/1/19 10:14
 */
public class JavaCustomNonParallelSourceFunction implements SourceFunction<Long> {

    private Long count = 1L;
    private boolean isRunning = true;

    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
        while (isRunning) {
            ctx.collect(count);
            count += 1;
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
