package com.song.flink.course4;

import com.song.flink.pojo.People;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.List;

/**
 * @author songshiyu
 * @date 2020/1/9 12:38
 */
public class JavaDataSetSourceApp {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        //fromCollection(environment);
        //fromTextFile(environment);
        //fromCsvFile(environment);
        readRecursiveFiles(environment);
    }

    //collection
    public static void fromCollection(ExecutionEnvironment environment) throws Exception {
        List<Integer> list = new ArrayList<Integer>();
        for (int i = 1; i <= 10; i++) {
            list.add(i);
        }
        environment.fromCollection(list).print();
    }

    public static void fromTextFile(ExecutionEnvironment env) throws Exception {
        String filePath = "file:///D:/Git/flink-train/data/04/text";
        env.readTextFile(filePath).print();
        System.out.println("--------------line-----------------");
        env.readTextFile("file:///D:/Git/flink-train/data/04/text/hello.txt").print();
    }

    public static void fromCsvFile(ExecutionEnvironment env) throws Exception {
        String filePath = "file:///D:/Git/flink-train/data/04/csv/people.csv";
        env.readCsvFile(filePath)
                .ignoreFirstLine()
                .includeFields(true,true,true)
                .pojoType(People.class,"name","age","work")
                .print();
    }

    public static void readRecursiveFiles(ExecutionEnvironment env) throws Exception {
        String filePath = "file:///D:/Git/flink-train/data/04/recursive";
        Configuration configuration = new Configuration();
        configuration.setBoolean("recursive.file.enumeration",true);
        env.readTextFile(filePath).withParameters(configuration).print();
    }
}
