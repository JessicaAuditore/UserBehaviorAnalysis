package com.limitofsoul.analysis;

import com.limitofsoul.beans.AdClinkEvent;
import com.limitofsoul.beans.AdCountViewByProvince;
import com.limitofsoul.beans.BlackListUserWarning;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Timestamp;
import java.util.Objects;

// 每5分钟统计1个小时内基于省份的广告点击量
public class AdStatisticsByProvince {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<AdClinkEvent> inputStream = env.readTextFile(Objects.requireNonNull(AdStatisticsByProvince.class.getResource("/AdClickLog.csv")).getPath())
                .map(line -> {
                    String[] fields = line.split(",");
                    return new AdClinkEvent(new Long(fields[0]), new Long(fields[1]), fields[2], fields[3], new Long(fields[4]));
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<AdClinkEvent>() {
                    @Override
                    public long extractAscendingTimestamp(AdClinkEvent adClinkEvent) {
                        return adClinkEvent.getTimestamp() * 1000L;
                    }
                });

        // 对同一个用户点击同一个广告的行为进行检查报警
        SingleOutputStreamOperator<AdClinkEvent> filterAdClickStream = inputStream.keyBy("userId", "adId") // 基于用户id和广告id分组
                .process(new FilterBlackListUser(100));

        // 基于省份 开窗聚合
        DataStream<AdCountViewByProvince> resultStream = filterAdClickStream.keyBy(AdClinkEvent::getProvince).timeWindow(Time.hours(1), Time.minutes(5))
                .aggregate(new AdCountAgg(), new AdCountResult());

        filterAdClickStream.getSideOutput(new OutputTag<BlackListUserWarning>("blackList") {}).print("black-user");
        resultStream.print();
        env.execute();
    }

    static class AdCountAgg implements AggregateFunction<AdClinkEvent, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(AdClinkEvent adClinkEvent, Long aLong) {
            return aLong + 1;
        }

        @Override
        public Long getResult(Long aLong) {
            return aLong;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return aLong + acc1;
        }
    }

    static class AdCountResult implements WindowFunction<Long, AdCountViewByProvince, String, TimeWindow> {
        @Override
        public void apply(String s, TimeWindow timeWindow, Iterable<Long> iterable, Collector<AdCountViewByProvince> collector) throws Exception {
            collector.collect(new AdCountViewByProvince(s, new Timestamp(timeWindow.getEnd()).toString(), iterable.iterator().next()));
        }
    }

    static class FilterBlackListUser extends KeyedProcessFunction<Tuple, AdClinkEvent, AdClinkEvent> {
        private Integer countUpperBound;

        public FilterBlackListUser(Integer countUpperBound) {
            this.countUpperBound = countUpperBound;
        }

        // 保存当前用户对某一广告的点击次数
        private ValueState<Long> countState;
        // 保存当前用户是否在黑名单
        private ValueState<Boolean> isSentState;

        @Override
        public void open(Configuration parameters) throws Exception {
            this.countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ad-count", Long.class, 0L));
            this.isSentState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("is-sent", Boolean.class, false));
        }

        @Override
        public void processElement(AdClinkEvent adClinkEvent, KeyedProcessFunction<Tuple, AdClinkEvent, AdClinkEvent>.Context context, Collector<AdClinkEvent> collector) throws Exception {
            Long curCount = this.countState.value();
            // 判断是否是第一个数据，如果是的话，注册一个第二天0点的定时器
            if (curCount == 0) {
                Long ts = (context.timerService().currentProcessingTime() / (24 * 60 * 60 * 1000) + 1) * (24 * 60 * 60 * 1000) - 8 * 60 * 60 * 1000;
                context.timerService().registerEventTimeTimer(ts);
            }

            // 判断当前用户对同一广告的点击次数，如果不够上限，就count+1正常输出，如果达到上限，直接过滤掉，输出到侧数据流并加入黑名单
            if (!this.isSentState.value() && curCount >= this.countUpperBound) {
                this.isSentState.update(false);
                context.output(new OutputTag<BlackListUserWarning>("blackList") {}, new BlackListUserWarning(adClinkEvent.getUserId(), adClinkEvent.getAdId(), "click over " + this.countUpperBound + "times."));
                return;
            }
            this.countState.update(curCount + 1);
            collector.collect(adClinkEvent);
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Tuple, AdClinkEvent, AdClinkEvent>.OnTimerContext ctx, Collector<AdClinkEvent> out) throws Exception {
            this.countState.clear();
            this.isSentState.clear();
        }
    }
}
