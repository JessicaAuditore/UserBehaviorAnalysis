package com.limitofsoul.analysis;

import com.limitofsoul.beans.PageViewCount;
import com.limitofsoul.beans.UserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Objects;
import java.util.Random;

// 统计1小时内网络浏览量pv
public class PageView {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(4);
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

        // 过滤分组开窗聚合，得到每个窗口内各个商品的count
        DataStream<PageViewCount> pvStream = dataStream.filter(data -> "pv".equals(data.getBehavior()))
                .map(new MapFunction<UserBehavior, Tuple2<Integer, Long>>() {
                    @Override
                    public Tuple2<Integer, Long> map(UserBehavior userBehavior) throws Exception {
                        return new Tuple2<>(new Random().nextInt(10), 1L);
                    }
                })
                .keyBy(data -> data.f0)
                .timeWindow(Time.hours(1))
                .aggregate(new PvCountAgg(), new PvCountResult());

        // 将个分区数据汇总起来
        DataStream<PageViewCount> pvResultStream = pvStream.keyBy(PageViewCount::getWindowEnd)
                .process(new KeyedProcessFunction<Long, PageViewCount, PageViewCount>() {
                    // 定义状态，保存总的count值
                    private ValueState<Long> totalCountState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        this.totalCountState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("total-count", Long.class, 0L));
                    }

                    @Override
                    public void processElement(PageViewCount pageViewCount, KeyedProcessFunction<Long, PageViewCount, PageViewCount>.Context context, Collector<PageViewCount> collector) throws Exception {
                        this.totalCountState.update(this.totalCountState.value() + pageViewCount.getCount());
                        context.timerService().registerEventTimeTimer(pageViewCount.getWindowEnd() + 1);
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<Long, PageViewCount, PageViewCount>.OnTimerContext ctx, Collector<PageViewCount> out) throws Exception {
                        out.collect(new PageViewCount("pv", ctx.getCurrentKey(), this.totalCountState.value()));
                        this.totalCountState.clear();
                    }
                });

        pvResultStream.print();
        env.execute();
    }

    static class PvCountAgg implements AggregateFunction<Tuple2<Integer, Long>, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Tuple2<Integer, Long> integerLongTuple2, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    static class PvCountResult implements WindowFunction<Long, PageViewCount, Integer, TimeWindow> {
        @Override
        public void apply(Integer integer, TimeWindow timeWindow, Iterable<Long> iterable, Collector<PageViewCount> collector) throws Exception {
            collector.collect(new PageViewCount(integer.toString(), timeWindow.getEnd(), iterable.iterator().next()));
        }
    }
}
