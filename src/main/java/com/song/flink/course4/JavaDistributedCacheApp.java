package com.song.flink.course4;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;

import java.io.File;
import java.util.List;

/**
 * @author songshiyu
 * @date 2020/1/17 9:09
 */
public class JavaDistributedCacheApp {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String filePath = "file:///D:/Git/flink-train/data/04/text/hello.txt";

        //step1:注册一个本地文件/HDFS文件
        env.registerCachedFile(filePath,"java-dc");

        DataSource<String> data = env.fromElements("hadoop", "spark", "flink", "java", "hive", "hbase");

        data.map(new RichMapFunction<String,String>() {

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                File file = getRuntimeContext().getDistributedCache().getFile("java-dc");
                List<String> lines = FileUtils.readLines(file);
                lines.stream().forEach(System.out::println);
            }

            @Override
            public String map(String value) throws Exception {
                return value;
            }
        }).print();

    }
}
