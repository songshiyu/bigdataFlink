package com.song.flink.hbase;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.hadoop.mapreduce.HadoopOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.util.ArrayList;
import java.util.List;

/**
 * @author songshiyu
 * @date 2020/2/5 14:11
 **/
public class HbaseSink {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        List<Tuple4<String,String,String,String>> list = new ArrayList<Tuple4<String,String,String,String>>();

        for (int i = 0; i < 10; i++){
            list.add(new Tuple4<String, String, String, String>(i + "","songhshiyu" + i,(30 + i) + "","beijing" + i));
        }
        DataSource<Tuple4<String, String, String, String>> info = env.fromCollection(list);

        //将数据源转化为hbase所需格式
        MapOperator<Tuple4<String, String, String, String>, Tuple2<Text, Mutation>> result = convertToHbase(info);

        //获取hbase客户端连接
        Configuration configuration = getHbaseConfiguration();

        Job job = Job.getInstance(configuration);

        //向hbase中写
        result.output(new HadoopOutputFormat<Text, Mutation>(new TableOutputFormat<Text>(),job));
        env.execute("HbaseSink");
    }

    private static Configuration getHbaseConfiguration() {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","hadoop000");
        configuration.set("habse.zookeeper.property.clientPort","2181");
        configuration.set(TableOutputFormat.OUTPUT_TABLE,"song_student");
        configuration.set("mapreduce.output.fileoutputformat.outputdir","/tmp");
        return configuration;
    }

    /**
     *  将数据转为hbase中的数据结构
     * */
    private static MapOperator<Tuple4<String, String, String, String>, Tuple2<Text, Mutation>> convertToHbase(DataSource<Tuple4<String, String, String, String>> info) {

        return info.map(new RichMapFunction<Tuple4<String, String, String, String>, Tuple2<Text, Mutation>>() {
            @Override
            public Tuple2<Text, Mutation> map(Tuple4<String, String, String, String> value) throws Exception {
                String cf = "o";

                String id = value.f0;
                String name = value.f1;
                String age = value.f2;
                String city = value.f3;

                Put put = new Put(id.getBytes());

                if (StringUtils.isNotEmpty(name)) {
                    put.addColumn(cf.getBytes(), "name".getBytes(), name.getBytes());
                }

                if (StringUtils.isNotEmpty(age + "")) {
                    put.addColumn(cf.getBytes(), "age".getBytes(), age.getBytes());
                }

                if (StringUtils.isNotEmpty(city)) {
                    put.addColumn(cf.getBytes(), "city".getBytes(), city.getBytes());
                }
                return new Tuple2<>(new Text(id), put);
            }
        });
    }
}
