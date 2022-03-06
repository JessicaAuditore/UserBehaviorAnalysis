package com.limitofsoul.analysis;

import com.limitofsoul.beans.UserBehavior;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

// 每5分钟统计一次1小时内的热门商品
public class HotItemsWithSql {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<String> inputStream = env.readTextFile("HotItemsAnalysis/src/main/resources/UserBehavior.csv");

        DataStream<UserBehavior> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return  new UserBehavior(new Long(fields[0]), new Long(fields[1]), new Integer(fields[2]), fields[3], new Long(fields[4]));
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
            @Override
            public long extractAscendingTimestamp(UserBehavior userBehavior) {
                return userBehavior.getTimestamp() * 1000L;
            }
        });

        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // 将流转换成表
        Table dataTable = tableEnv.fromDataStream(dataStream, "itemId, behavior, timestamp.rowtime as ts");

        // 分组开窗
        // 1)table api
        Table windowAggTable = dataTable.filter("behavior = 'pv'")
                .window(Slide.over("1.hours").every("5.minutes").on("ts").as("w"))
                .groupBy("itemId, w")
                .select("itemId, w.end as windowEnd, itemId.count as cnt");

        // 开窗函数对count值进行排序并获取Row number，得到TopN
        // sql
        DataStream<Row> aggStream = tableEnv.toAppendStream(windowAggTable, Row.class);
        tableEnv.createTemporaryView("agg", aggStream, "itemId, windowEnd, cnt");

        Table resultTable = tableEnv.sqlQuery("select * from" +
                " (select *, ROW_NUMBER() over (partition by windowEnd order by cnt desc) as row_num from agg)" +
                " where row_num <= 5");
//        tableEnv.toRetractStream(resultTable, Row.class).print();

        // 2) sql
        tableEnv.createTemporaryView("data_table", dataStream, "itemId, behavior, timestamp.rowtime as ts");
        Table resultSqlTable = tableEnv.sqlQuery("select * from" +
                " (select *, ROW_NUMBER() over (partition by windowEnd order by cnt desc) as row_num from " +
                "(" +
                "select itemId, count(itemId) as cnt, HOP_END(ts, interval '5' minute, interval '1' hour) as windowEnd" +
                " from data_table where behavior = 'pv' group by itemId, HOP(ts, interval '5' minute, interval '1' hour))" +
                ")" +
                " where row_num <= 5");

        tableEnv.toRetractStream(resultSqlTable, Row.class).print();

        env.execute();
    }
}
