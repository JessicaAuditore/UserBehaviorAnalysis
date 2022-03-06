package com.limitofsoul.analysis;

import com.limitofsoul.beans.ChannelPromotionCount;
import com.limitofsoul.beans.MarketingUserBehavior;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

// 不分渠道统计市场安装
public class AppMarketingStatistics {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<MarketingUserBehavior> dataStream = env.addSource(new AppMarketingByChannel.SimulatedMarketingUserBehaviorSource())
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<MarketingUserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(MarketingUserBehavior marketingUserBehavior) {
                        return marketingUserBehavior.getTimestamp();
                    }
                });

        // 开窗统计总量
        DataStream<ChannelPromotionCount> resultStream = dataStream
                .filter(data -> !"UNINSTALL".equals(data.getBehavior()))
                .timeWindowAll(Time.hours(1), Time.seconds(5))
                .apply(new AllWindowFunction<MarketingUserBehavior, ChannelPromotionCount, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<MarketingUserBehavior> iterable, Collector<ChannelPromotionCount> collector) throws Exception {
                        collector.collect(new ChannelPromotionCount("total", "total", new Timestamp(timeWindow.getEnd()).toString(), (long) IteratorUtils.toList(iterable.iterator()).size()));
                    }
                });
//                .map(new MapFunction<MarketingUserBehavior, Tuple2<String, Long>>() {
//                    @Override
//                    public Tuple2<String, Long> map(MarketingUserBehavior marketingUserBehavior) throws Exception {
//                        return new Tuple2<>("total", 1L);
//                    }
//                })
//                .keyBy(0)
//                .timeWindow(Time.hours(1), Time.seconds(5))
//                .aggregate(new MarketingStatisticsAgg(), new MarketingStatisticsResult());

        resultStream.print();
        env.execute();
    }

    static class MarketingStatisticsAgg implements AggregateFunction<Tuple2<String, Long>, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Tuple2<String, Long> stringLongTuple2, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long aLong) {
            return aLong;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    static class MarketingStatisticsResult implements WindowFunction<Long, ChannelPromotionCount, Tuple, TimeWindow> {
        @Override
        public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Long> iterable, Collector<ChannelPromotionCount> collector) throws Exception {
            collector.collect(new ChannelPromotionCount("total", "total", new Timestamp(timeWindow.getEnd()).toString(), iterable.iterator().next()));
        }
    }
}
