package com.song.flink.course07;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * ReduceFunction
 * */
public class JavaWindowsReduceApp {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> text = env.socketTextStream("localhost", 9999);

        text.flatMap(new FlatMapFunction<String,Tuple2<Integer,Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<Integer, Integer>> collector) throws Exception {
                String[] splits = s.toLowerCase().split("\\,");
                for (String token:splits){
                    if (token.length() > 0){
                        collector.collect(new Tuple2<>(1,Integer.parseInt(token)));
                    }
                }
            }
        }).keyBy(0)
                .timeWindow(Time.seconds(5))
                .reduce(new ReduceFunction<Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> value1,
                                                           Tuple2<Integer, Integer> value2) throws Exception {
                        System.out.println(value1 + "..." + value2);
                        return new Tuple2<>(value1.f0,value1.f1 + value2.f1);
                    }
                })
                .print()
                .setParallelism(1);

        env.execute("JavaWindowsApp");
    }
}
