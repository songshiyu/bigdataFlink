package com.song.flink.course4;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.expressions.In;
import org.apache.flink.util.Collector;
import util.DBUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author songshiyu
 * @date 2020/1/13 9:03
 */
public class JavaDataSetTransformationApp {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //mapFunction(env);
        //filterFunction(env);
        //mapPartititonFunction(env);
        //firstFunction(env);
        //flatMapFunction(env);
        //joinFunction(env);
        //leftOuterJoinFunction(env);
        //rightOuterJoinFunction(env);
        //fullOuterJoinFunction(env);
        crossFunction(env);
    }

    /**
     * map 操作
     */
    public static void mapFunction(ExecutionEnvironment env) throws Exception {
        DataSource<Integer> data = env.fromCollection(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        data.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer integer) throws Exception {
                return integer + 1;
            }
        }).print();

        //data.map(x -> x + 1).print();
    }

    /**
     * filter操作
     */
    public static void filterFunction(ExecutionEnvironment env) throws Exception {

        env.fromCollection(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
                .map(x -> x + 1)
                .filter(new FilterFunction<Integer>() {
                    @Override
                    public boolean filter(Integer integer) throws Exception {
                        return integer > 5;
                    }
                }).print();
       /* env.fromCollection(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
                .map(x -> x + 1).filter(x -> x > 5).print();*/
    }

    /**
     * MapPartition
     **/
    public static void mapPartititonFunction(ExecutionEnvironment env) throws Exception {
        List<String> list = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            list.add(i + "");
        }
        env.fromCollection(list).setParallelism(6)
                .mapPartition(new MapPartitionFunction<String, String>() {
                    @Override
                    public void mapPartition(Iterable<String> iterable, Collector<String> collector) throws Exception {
                      /*  Iterator<String> iterator = iterable.iterator();
                        while (iterator.hasNext()){
                            String line = iterator.next();
                            collector.collect((Integer.valueOf(line) + 2) + "");
                        }*/
                        String connection = DBUtils.getConnection();
                        System.out.println("获取到数据库链接：" + connection);
                        DBUtils.returnConnection(connection);
                    }
                }).print();
    }

    /**
     * first group sortGroup
     */
    public static void firstFunction(ExecutionEnvironment env) throws Exception {
        List<Tuple2<Integer, String>> info = new ArrayList<Tuple2<Integer, String>>();

        info.add(new Tuple2<>(1, "hadoop"));
        info.add(new Tuple2<>(1, "spark"));
        info.add(new Tuple2<>(1, "flink"));
        info.add(new Tuple2<>(2, "java"));
        info.add(new Tuple2<>(2, "php"));
        info.add(new Tuple2<>(2, "scala"));
        info.add(new Tuple2<>(2, "js"));
        info.add(new Tuple2<>(3, "vue"));
        info.add(new Tuple2<>(3, "jquery"));
        info.add(new Tuple2<>(3, "receat"));

        //普通取前三条
        //env.fromCollection(info).first(3).print();

        //分组取前2条
        //env.fromCollection(info).groupBy(0).first(2).print();

        //分组排序取前两条
        env.fromCollection(info).groupBy(0).sortGroup(1, Order.DESCENDING).first(3).print();
    }

    public static void flatMapFunction(ExecutionEnvironment env) throws Exception {
        List<String> info = Arrays.asList("hadoop,spark", "flink,java", "vue,java", "hadoop,spark");

        env.fromCollection(info).flatMap(new FlatMapFunction<String,String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] splits = s.split("\\,");
                Arrays.stream(splits).forEach(e -> {
                    collector.collect(e);
                });
            }
        }).map(new MapFunction<String,Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return new Tuple2<>(s,1);
            }
        }).groupBy(0).sum(1).print();
    }

    public static void distinctFunction(ExecutionEnvironment env) throws Exception {
        List<String> info = Arrays.asList("hadoop,spark", "flink,java", "vue,java", "hadoop,spark");

        env.fromCollection(info).flatMap(new FlatMapFunction<String,String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] splits = s.split("\\,");
                Arrays.stream(splits).forEach(e -> {
                    collector.collect(e);
                });
            }
        }).distinct().print();
    }

    public static void joinFunction(ExecutionEnvironment env) throws Exception{
        List<Tuple2<Integer, String>> info1 = new ArrayList<Tuple2<Integer, String>>();

        info1.add(new Tuple2<>(1, "hadoop"));
        info1.add(new Tuple2<>(2, "spark"));
        info1.add(new Tuple2<>(3, "flink"));
        info1.add(new Tuple2<>(5, "java"));
        DataSource<Tuple2<Integer, String>> data1 = env.fromCollection(info1);

        List<Tuple2<Integer, String>> info2 = new ArrayList<Tuple2<Integer, String>>();

        info2.add(new Tuple2<>(1, "北京"));
        info2.add(new Tuple2<>(2, "上海"));
        info2.add(new Tuple2<>(3, "石家庄"));
        info2.add(new Tuple2<>(4, "新乐"));
        DataSource<Tuple2<Integer, String>> data2 = env.fromCollection(info2);

        data1.join(data2).where(0).equalTo(0)
                .with(new JoinFunction<Tuple2<Integer,String>, Tuple2<Integer,String>, Tuple3<Integer,String,String>>() {
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                        return new Tuple3<>(first.f0,first.f1,second.f1);
                    }
                }).print();
    }

    public static void leftOuterJoinFunction(ExecutionEnvironment env) throws Exception{
        List<Tuple2<Integer, String>> info1 = new ArrayList<Tuple2<Integer, String>>();

        info1.add(new Tuple2<>(1, "hadoop"));
        info1.add(new Tuple2<>(2, "spark"));
        info1.add(new Tuple2<>(3, "flink"));
        info1.add(new Tuple2<>(5, "java"));
        DataSource<Tuple2<Integer, String>> data1 = env.fromCollection(info1);

        List<Tuple2<Integer, String>> info2 = new ArrayList<Tuple2<Integer, String>>();

        info2.add(new Tuple2<>(1, "北京"));
        info2.add(new Tuple2<>(2, "上海"));
        info2.add(new Tuple2<>(3, "石家庄"));
        info2.add(new Tuple2<>(4, "新乐"));
        DataSource<Tuple2<Integer, String>> data2 = env.fromCollection(info2);

        data1.leftOuterJoin(data2).where(0).equalTo(0)
                .with(new JoinFunction<Tuple2<Integer,String>, Tuple2<Integer,String>, Tuple3<Integer,String,String>>() {
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                        if (second == null){
                            return new Tuple3<>(first.f0,first.f1,"-");
                        }else {
                            return new Tuple3<>(first.f0,first.f1,second.f1);
                        }
                    }
                }).print();
    }

    public static void rightOuterJoinFunction(ExecutionEnvironment env) throws Exception{
        List<Tuple2<Integer, String>> info1 = new ArrayList<Tuple2<Integer, String>>();

        info1.add(new Tuple2<>(1, "hadoop"));
        info1.add(new Tuple2<>(2, "spark"));
        info1.add(new Tuple2<>(3, "flink"));
        info1.add(new Tuple2<>(5, "java"));
        DataSource<Tuple2<Integer, String>> data1 = env.fromCollection(info1);

        List<Tuple2<Integer, String>> info2 = new ArrayList<Tuple2<Integer, String>>();

        info2.add(new Tuple2<>(1, "北京"));
        info2.add(new Tuple2<>(2, "上海"));
        info2.add(new Tuple2<>(3, "石家庄"));
        info2.add(new Tuple2<>(4, "新乐"));
        DataSource<Tuple2<Integer, String>> data2 = env.fromCollection(info2);

        data1.rightOuterJoin(data2).where(0).equalTo(0)
                .with(new JoinFunction<Tuple2<Integer,String>, Tuple2<Integer,String>, Tuple3<Integer,String,String>>() {
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                        if (first == null){
                            return new Tuple3<>(second.f0,"-",second.f1);
                        }else {
                            return new Tuple3<>(first.f0,first.f1,second.f1);
                        }
                    }
                }).print();
    }

    public static void fullOuterJoinFunction(ExecutionEnvironment env) throws Exception{
        List<Tuple2<Integer, String>> info1 = new ArrayList<Tuple2<Integer, String>>();

        info1.add(new Tuple2<>(1, "hadoop"));
        info1.add(new Tuple2<>(2, "spark"));
        info1.add(new Tuple2<>(3, "flink"));
        info1.add(new Tuple2<>(5, "java"));
        DataSource<Tuple2<Integer, String>> data1 = env.fromCollection(info1);

        List<Tuple2<Integer, String>> info2 = new ArrayList<Tuple2<Integer, String>>();

        info2.add(new Tuple2<>(1, "北京"));
        info2.add(new Tuple2<>(2, "上海"));
        info2.add(new Tuple2<>(3, "石家庄"));
        info2.add(new Tuple2<>(4, "新乐"));
        DataSource<Tuple2<Integer, String>> data2 = env.fromCollection(info2);

        data1.fullOuterJoin(data2).where(0).equalTo(0)
                .with(new JoinFunction<Tuple2<Integer,String>, Tuple2<Integer,String>, Tuple3<Integer,String,String>>() {
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                        if (first == null){
                            return new Tuple3<>(second.f0,"-",second.f1);
                        }else if (second == null){
                            return new Tuple3<>(first.f0,first.f1,"-");
                        }else {
                            return new Tuple3<>(first.f0,first.f1,second.f1);
                        }
                    }
                }).print();
    }

    public static void crossFunction(ExecutionEnvironment env) throws Exception{
        List<String> info1 = Arrays.asList("曼联", "曼城");
        List<Integer> info2 = Arrays.asList(3,2,1);

        DataSource<String> data1 = env.fromCollection(info1);
        DataSource<Integer> data2 = env.fromCollection(info2);

        data1.cross(data2).print();
    }
}
