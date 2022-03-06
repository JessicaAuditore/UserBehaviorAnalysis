package com.limitofsoul.analysis;

import com.limitofsoul.beans.PageViewCount;
import com.limitofsoul.beans.UserBehavior;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

// 统计1小时内网站独立访客数uv
public class UniqueVisitor {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<String> inputStream = env.readTextFile(Objects.requireNonNull(PageView.class.getResource("/UserBehavior.csv")).getPath());
        DataStream<UserBehavior> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new UserBehavior(new Long(fields[0]), new Long(fields[1]), new Integer(fields[2]), fields[3], new Long(fields[4]));
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
            @Override
            public long extractAscendingTimestamp(UserBehavior userBehavior) {
                return userBehavior.getTimestamp() * 1000L;
            }
        });

        // 开窗统计uv值
        DataStream<PageViewCount> uvStream = dataStream.filter(data -> "pv".equals(data.getBehavior()))
                .timeWindowAll(Time.hours(1))
                .apply(new UvCountResult());

        uvStream.print();
        env.execute();
    }

    static class UvCountResult implements AllWindowFunction<UserBehavior, PageViewCount, TimeWindow> {

        @Override
        public void apply(TimeWindow timeWindow, Iterable<UserBehavior> iterable, Collector<PageViewCount> collector) throws Exception {
            // 定义set保存窗口中的所有数据userId
            Set<Long> uidSet = new HashSet<>();
            for (UserBehavior ub : iterable) {
                uidSet.add(ub.getUserId());
            }
            collector.collect(new PageViewCount("uv", timeWindow.getEnd(), (long) uidSet.size()));
        }
    }
}
