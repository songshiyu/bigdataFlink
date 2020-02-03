package com.song.flink.project;

import com.song.flink.project.constant.KafkaConstant;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import scala.Tuple4;

import java.text.SimpleDateFormat;
import java.util.Properties;

/**
 * @author songshiyu
 * @date 2020/2/3 15:18
 **/
@Slf4j
public class LogAnalysis {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //接收kafka数据
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", KafkaConstant.BOOTSTARTSTRAP_SERVERS);
        properties.setProperty("group.id", "test-group");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>(KafkaConstant.TOPIC, new SimpleStringSchema(), properties);

        //接收kafka数据
        DataStreamSource<String> data = env.addSource(consumer);

        SingleOutputStreamOperator<Tuple4<String, Long, String, String>> mapData =
                data.map(new MapFunction<String, Tuple4<String, Long, String, String>>() {
                    public Tuple4<String, Long, String, String> map(String value) throws Exception {
                        String[] splits = value.split("\t", -1);
                        Tuple4<String, Long, String, String> tuple4 = null;
                        try {
                            String level = splits[2];
                            String timeStr = splits[3];
                            Long time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(timeStr).getTime();
                            String domain = splits[5];
                            String traffic = splits[6];
                            tuple4 = new Tuple4<String, Long, String, String>(level, time, domain, traffic);
                        } catch (Exception e) {
                            log.error("日志格式不标准:{}", value);
                        }
                        return tuple4;
                    }
                });

        mapData.print().setParallelism(1);

        env.execute("LogAnalysis");
    }
}
