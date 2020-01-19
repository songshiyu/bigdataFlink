package com.song.flink.course05;

import com.song.flink.pojo.Student;
import com.song.flink.util.Sink2Mysql;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author songshiyu
 * @date 2020/1/19 13:50
 */
public class JavaCustomSink2Mysql {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> data = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<Student> studentStream = data.map(new MapFunction<String, Student>() {
            @Override
            public Student map(String value) throws Exception {
                Student stu = new Student();
                String[] splits = value.split("\\,", -1);
                if (splits.length >= 3) {
                    stu.setId(Integer.parseInt(splits[0]));
                    stu.setName(splits[1]);
                    stu.setAge(Integer.parseInt(splits[2]));
                }
                return stu;
            }
        });

        studentStream.addSink(new Sink2Mysql());

        env.execute("JavaCustomSink2Mysql");
    }
}
