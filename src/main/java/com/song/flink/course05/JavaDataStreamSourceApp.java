package com.song.flink.course05;


import com.song.flink.util.JavaCustomNonParallelSourceFunction;
import com.song.flink.util.JavaCustomParallelSourceFunction;
import com.song.flink.util.JavaCustomRichParallelSourceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author songshiyu
 * @date 2020/1/19 9:16
 */
public class JavaDataStreamSourceApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //socketFunction(env);
        //customNonParallelSourceFunction(env);
        //customParallelSourceFunction(env);
        customRichParallelSourceFunction(env);

        env.execute("JavaDataStreamSourceApp");
    }

    public static void socketFunction(StreamExecutionEnvironment env){
        DataStreamSource<String> data = env.socketTextStream("localhost", 9999);
        data.print().setParallelism(1);
    }

    public static void customNonParallelSourceFunction(StreamExecutionEnvironment env){
        DataStreamSource<Long> dataSource = env.addSource(new JavaCustomNonParallelSourceFunction()).setParallelism(1);
        dataSource.print().setParallelism(1);
    }

    public static void customRichParallelSourceFunction(StreamExecutionEnvironment env){
        DataStreamSource<Long> dataSource = env.addSource(new JavaCustomRichParallelSourceFunction()).setParallelism(2);
        dataSource.print().setParallelism(2);
    }

    public static void customParallelSourceFunction(StreamExecutionEnvironment env){
        DataStreamSource<Long> dataSource = env.addSource(new JavaCustomParallelSourceFunction()).setParallelism(2);
        dataSource.print().setParallelism(2);
    }
}
