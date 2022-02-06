package com.zdautomotive.cloud.tools;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.Point.Builder;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.influxdb.dto.QueryResult.*;

import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class InfluxJDBC {
    private String username;
    private String password;
    private String url;
    private String database;
    private InfluxDB influxDB;

    public InfluxJDBC() {
        //TODO 获取配置文件
        Properties properties = new Properties();
        InputStream inputStream = Object.class.getResourceAsStream("/influxDB.properties");
        try {
            properties.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        //TODO 字段赋予值，链接数据库
        username = properties.get("username").toString();
        password = properties.get("password").toString();
        url = "http://" + properties.get("hostname").toString() + ":" + properties.get("port").toString();
        database = properties.get("database").toString();
        influxDB = InfluxDBFactory.connect(url, username, password);
    }

    public void insert(String measurement, Map<String, String> tags, Map<String, Object> fields) {
        //这里是api Point中的Builder，用于构建measurement，既构建表结构
        Builder builder = Point.measurement(measurement);
        //表结构中添加tag
        builder.tag(tags);
        //表结构中添加列
        builder.fields(fields);
        //写入
        influxDB.write(database, "", builder.build());

    }

    public void insert(String measurement, Map<String, String> tags, Map<String, Object> fields, String date) {
        //这里是api Point中的Builder，用于构建measurement，既构建表结构
        Builder builder = Point.measurement(measurement);
        //表结构中添加tag
        builder.tag(tags);
        //表结构中添加列
        builder.fields(fields);
        SimpleDateFormat sf = new SimpleDateFormat("yyyyMMddHH Z");
        //转换为long格式的时间戳
        long time = 0;
        try {
            time = sf.parse(date + " UTC").getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        //插入时间戳
        builder.time(time, TimeUnit.MILLISECONDS);
        //写入
        influxDB.write(database, "", builder.build());
    }

    public List<Map<String, Object>> query(String command) throws ParseException {
        List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
        //直接查询获取值Result,Result是所有结果的集合
        QueryResult qr = influxDB.query(new Query(command, database));
        //System.out.println(qr);
        //循环Result，获取每组的值
        for (Result r : qr.getResults()) {
            if (r == null) {
                throw new RuntimeException("没有查询结果Results");
            } else {
                //获取所有列的集合，这里的一个迭代是代表一组，而不是一行
                List<Series> series = r.getSeries();
                for (Series s : series) {
                    //列中含有多行数据，每行数据含有多列value，所以嵌套List
                    List<List<Object>> values = s.getValues();
                    //每组的列是固定的
                    List<String> columns = s.getColumns();
                    //每组的tags是固定的
                    //Map<String,String> tags = s.getTags();
                    for (List<Object> v : values) {
                        //循环遍历结果集，获取每行对应的value，以map形式保存
                        Map<String, Object> queryMap = new HashMap<String, Object>();
                        for (int i = 0; i < columns.size(); i++) {
                            //遍历所有列名，获取列对应的值
                            String column = columns.get(i);
                            if (v.get(i) == null) {
                                //如果是null就存入null
                                queryMap.put(column, null);
                            } else {
                                //不是null就转成字符串存储
                                String value = v.get(i).toString();
                                //如果是时间戳还可以格式转换，我这里懒了
                                queryMap.put(column, value);
                            }
                        }
                        //把结果添加到结果集中
                        result.add(queryMap);
                    }
                }
            }
        }
        return result;
    }

}
