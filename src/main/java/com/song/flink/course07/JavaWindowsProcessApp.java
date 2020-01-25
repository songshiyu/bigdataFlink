package com.song.flink.course07;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * ProcessFunction
 */
public class JavaWindowsProcessApp {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> text = env.socketTextStream("localhost", 9999);

        text.flatMap(new FlatMapFunction<String, Tuple2<Integer, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<Integer, Integer>> collector) throws Exception {
                String[] splits = s.toLowerCase().split("\\,");
                for (String token : splits) {
                    if (token.length() > 0) {
                        collector.collect(new Tuple2<>(1, Integer.parseInt(token)));
                    }
                }
            }
        }).keyBy(0)
                .timeWindow(Time.seconds(5))
                //会等待该窗口内的数据到位之后，进行一次性的计算
                .process(new ProcessWindowFunction<Tuple2<Integer, Integer>, Object, Tuple, TimeWindow>() {
                    @Override
                    public void process(Tuple tuple, Context context, Iterable<Tuple2<Integer, Integer>> iterable,
                                        Collector<Object> collector) throws Exception {
                        long count = 0L;
                        for (Tuple2<Integer,Integer> t : iterable){
                            count++;
                        }
                        collector.collect("Window: " + context.window() + "count: " + count);
                    }
                })
                .print()
                .setParallelism(1);

        env.execute("JavaWindowsProcessApp");
    }
}
