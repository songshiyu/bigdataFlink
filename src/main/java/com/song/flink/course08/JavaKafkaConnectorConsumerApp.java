package com.song.flink.course08;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class JavaKafkaConnectorConsumerApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //chechpoint常用设置参数
        env.enableCheckpointing(4000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        String topic = "test";
        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers", "192.168.137.10:9092");
        properties.setProperty("group.id", "test");

        DataStreamSource<String> data = env.addSource(new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), properties));
        data.print();
        env.execute("JavaKafkaConnectorConsumerApp");
    }
}
