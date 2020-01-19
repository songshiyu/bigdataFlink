package com.song.flink.util;

import com.song.flink.pojo.Student;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @author songshiyu
 * @date 2020/1/19 14:00
 */
public class Sink2Mysql extends RichSinkFunction<Student>{

    private Connection connection;
    private PreparedStatement pstmt;

    /**
     * 获取数据库连接
     * */
    public  Connection getConnection() {

        Connection conn = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            String url = "jdbc:mysql://localhost:3306/flink?characterEncoding=utf-8";
            conn = DriverManager.getConnection(url,"root","root");

        } catch (Exception e) {
            e.printStackTrace();
        }
        return conn;
    }

    /**
     * 在open方法中建立connection
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        connection = getConnection();
        String sql = "insert into student(id,name,age) values (?,?,?)";
        pstmt = connection.prepareStatement(sql);

        System.out.println("open");
    }

    // 每条记录插入时调用一次
    public void invoke(Student value, Context context) throws Exception {
        System.out.println("invoke~~~~~~~~~");
        // 未前面的占位符赋值
        pstmt.setInt(1, value.getId());
        pstmt.setString(2, value.getName());
        pstmt.setInt(3, value.getAge());

        pstmt.executeUpdate();
    }

    @Override
    public void close() throws Exception {
        super.close();

        if(pstmt != null) {
            pstmt.close();
        }

        if(connection != null) {
            connection.close();
        }
    }
}

