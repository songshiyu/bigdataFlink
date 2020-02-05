package com.song.flink.project;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;

/**
 * @author songshiyu
 * @date 2020/2/5 9:16
 **/
public class ProjectMysqlSource extends RichParallelSourceFunction<HashMap<String,String>> {

    Connection connection = null;
    PreparedStatement pst = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        String driver = "com.mysql.jdbc.Driver";
        Class.forName(driver);
        String url = "jdbc:mysql://192.168.137.10:3306/flink";
        String username = "root";
        String password = "root";

        connection = DriverManager.getConnection(url, username, password);

        String sql = "select user_id,domain from user_domain_config";
        pst = connection.prepareStatement(sql);
    }

    @Override
    public void run(SourceContext<HashMap<String,String>> ctx) throws Exception {
        //此处是核心所在
        ResultSet resultSet = pst.executeQuery();
        while (resultSet.next()) {
            String userId = resultSet.getString("user_id");
            String domain = resultSet.getString("domain");
            HashMap<String,String> map = new HashMap<>();
            map.put(domain,userId);
            ctx.collect(map);
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (pst != null) {
            pst.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    @Override
    public void cancel() {

    }
}
