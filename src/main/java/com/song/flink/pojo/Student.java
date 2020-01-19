package com.song.flink.pojo;

import lombok.Data;

/**
 * @author songshiyu
 * @date 2020/1/19 11:18
 */
@Data
public class Student {

    private int id;

    private String name;

    private int age;

    @Override
    public String toString() {
        return "Student{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", age=" + age +
                '}';
    }
}
