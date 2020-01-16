package com.song.flink.course4;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem;

import java.util.Arrays;
import java.util.List;

/**
 * @author songshiyu
 * @date 2020/1/16 8:52
 */
public class JavaSinkDataAPP {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        writeAsTextFunction(env);
        env.execute("JavaSinkDataAPP");
    }

    public static void writeAsTextFunction(ExecutionEnvironment env){
        List<Integer> info = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);
        String filePath = "file:///D:/home/flink/javaOutPut";
        env.fromCollection(info)
                .writeAsText(filePath, FileSystem.WriteMode.OVERWRITE).setParallelism(2);
    }
}
