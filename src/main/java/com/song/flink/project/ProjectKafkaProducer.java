package com.song.flink.project;

import com.song.flink.project.constant.KafkaConstant;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;

/**
 * @author songshiyu
 * @date 2020/2/3 12:52
 *
 *  Kafka生产者代码
 **/
@Slf4j
public class ProjectKafkaProducer {

    public static void main(String[] args) throws Exception{

        Properties properties = new Properties();
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty("bootstrap.servers", KafkaConstant.BOOTSTARTSTRAP_SERVERS);

        KafkaProducer<String,String> producer = new KafkaProducer<String,String>(properties);

        while (true){
            StringBuilder builder = new StringBuilder();

            builder.append("imooc").append("\t")
                    .append("CN").append("\t")
                    .append(getLevels()).append("\t")
                    /*.append("").append("\t")
                    .append("").append("\t")
                    .append("").append("\t")
                    .append("").append("\t")
                    .append("").append("\t")*/;

            log.info("builder ==>" + builder.toString());
            //producer.send(new ProducerRecord<String, String>(KafkaConstant.TOPIC,builder.toString()));

            Thread.sleep(2000L);
        }
    }

    public static String getLevels(){
        String[] levels = new String[]{"M","E"};
        return levels[new Random().nextInt(levels.length)];
    }
}
