package com.song.flink.course4;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;

import java.util.List;

/**
 * @author songshiyu
 * @date 2020/1/17 9:26
 */
public class JavaBroadCastApp {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<Integer> toBroadcast = env.fromElements(1, 2, 3);

        DataSource<String> data = env.fromElements("a", "b");

        data.map(new RichMapFunction<String,String>() {

            List<Integer> broadcastValue = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                broadcastValue = getRuntimeContext().getBroadcastVariable("broadcastSetName");
            }

            @Override
            public String map(String value) throws Exception {
                for (Integer integer : broadcastValue){
                    System.out.println("广播变量:" + integer);
                }
                return value;
            }
        }).withBroadcastSet(toBroadcast,"broadcastSetName").print();
    }
}
