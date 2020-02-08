package com.song.flink.hbase;

import org.apache.flink.addons.hbase.TableInputFormat;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author songshiyu
 * @date 2020/2/5 19:29
 **/
public class HbaseSource {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        byte[] cf = "o".getBytes();

        env.createInput(new TableInputFormat<Tuple4<String,String,Integer,String>>() {

            private HTable createTable() throws IOException {
                org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
                configuration.set("hbase.zookeeper.quorum", "hadoop000");
                configuration.set("hbase.zookeeper.property.clientPort", "2181");
                HTable table = new HTable(configuration, getTableName());
                return table;
            }

            @Override
            public void configure(Configuration parameters) {
                try {
                    table = createTable();
                    if (table != null) {
                        scan = getScanner();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            @Override
            protected Scan getScanner() {
                Scan scan = new Scan();
                scan.addFamily(cf);
                return scan;
            }

            @Override
            protected String getTableName() {
                return "song_students";
            }

            @Override
            protected Tuple4<String, String, Integer, String> mapResultToTuple(Result result) {
                String id = Bytes.toString(result.getRow());
                String name = Bytes.toString(result.getValue(cf,"name".getBytes()));
                Integer age = Bytes.toInt(result.getValue(cf,"age".getBytes()));
                String city = Bytes.toString(result.getValue(cf,"city".getBytes()));
                return new Tuple4<>(id,name,age,city);
            }
        });

        env.execute("HbaseSource");
    }
}
