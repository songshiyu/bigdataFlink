package com.song.flink.project;

import com.song.flink.project.constant.KafkaConstant;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.text.SimpleDateFormat;
import java.util.Date;
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
                    .append(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())).append("\t")
                    .append(getIps()).append("\t")
                    .append(getDomains()).append("\t")
                    .append(getTraffic()).append("\t");

            System.out.println("builder ==>" + builder.toString());
            producer.send(new ProducerRecord<String, String>(KafkaConstant.TOPIC,builder.toString()));

            Thread.sleep(2000L);
        }
    }

    public static String getIps(){
        String[] ips = new String[]{
                "223.104.18.110",
                "223.104.18.111",
                "223.104.18.112",
                "223.101.18.113",
                "223.105.18.114",
                "223.104.18.115",
                "223.108.18.116",
                "223.107.18.117",
                "223.106.18.118",
                "223.105.18.119"
        };
        return ips[new Random().nextInt(ips.length)];
    }


    public static String getLevels(){
        String[] levels = new String[]{"M","E"};
        return levels[new Random().nextInt(levels.length)];
    }

    //得到域名
    public static String getDomains(){
        String[] domains = new String[]{
                "v1.go2yd.com","v2.go2yd.com","v3.go2yd.com","v4.go2yd.com","vmi.go2yd.com"
        };
        return domains[new Random().nextInt(domains.length)];
    }

    //流量
    public static Long getTraffic(){
        return Long.parseLong(new Random().nextInt(10000) + "");
    }
}
