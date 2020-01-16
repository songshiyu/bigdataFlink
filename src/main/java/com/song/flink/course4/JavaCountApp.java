package com.song.flink.course4;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;

/**
 * @author songshiyu
 * @date 2020/1/16 9:56
 */
public class JavaCountApp {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> dataSource = env.fromElements("hadoop", "spark", "flink", "java", "hive", "hbase");

        DataSet<String> info = dataSource.map(new RichMapFunction<String,String>() {

            LongCounter counter = new LongCounter();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                getRuntimeContext().addAccumulator("ele-counts-java",counter);
            }

            @Override
            public String map(String value) throws Exception {
                counter.add(1);
                return value;
            }
        });

        String filePath = "file:///D:/home/flink/counter/java_counter";
        info.writeAsText(filePath, FileSystem.WriteMode.OVERWRITE);

        JobExecutionResult jobResult = env.execute("CounterApp");
        Long num = jobResult.getAccumulatorResult("ele-counts-java");

        System.out.println("num:" + num);
    }
}
