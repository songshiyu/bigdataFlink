package com.song.flink.project;

import com.song.flink.project.constant.KafkaConstant;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @author songshiyu
 * @date 2020/2/3 15:18
 **/
public class LogAnalysis {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //接收kafka数据
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers",KafkaConstant.BOOTSTARTSTRAP_SERVERS);
        properties.setProperty("group.id","test-group");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>(KafkaConstant.TOPIC,new SimpleStringSchema(),properties);

        //接收kafka数据
        DataStreamSource<String> data = env.addSource(consumer);

        data.print().setParallelism(1);

        env.execute("LogAnalysis");
    }
}
