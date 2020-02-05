package com.song.flink;

import com.song.flink.project.ProjectMysqlSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;

/**
 * @author songshiyu
 * @date 2020/1/9 9:17
 */
public class test {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStreamSource<HashMap<String, String>> dataStreamSource = env.addSource(new ProjectMysqlSource());

        dataStreamSource.print().setParallelism(1);

        env.execute("test");
    }
}
