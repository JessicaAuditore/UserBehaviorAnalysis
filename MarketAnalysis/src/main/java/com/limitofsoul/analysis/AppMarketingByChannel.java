package com.limitofsoul.analysis;

import com.limitofsoul.beans.ChannelPromotionCount;
import com.limitofsoul.beans.MarketingUserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

// 分渠道统计市场
public class AppMarketingByChannel {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<MarketingUserBehavior> dataStream = env.addSource(new SimulatedMarketingUserBehaviorSource())
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<MarketingUserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(MarketingUserBehavior marketingUserBehavior) {
                        return marketingUserBehavior.getTimestamp();
                    }
                });

        // 分渠道开窗统计
        DataStream<ChannelPromotionCount> resultStream = dataStream
                .filter(data -> !"UNINSTALL".equals(data.getBehavior()))
                .keyBy("channel", "behavior")
                .timeWindow(Time.hours(1), Time.seconds(5))
                .aggregate(new MarketingCountAgg(), new MarketingCountResult());

        resultStream.print();
        env.execute();
    }

    static class SimulatedMarketingUserBehaviorSource implements SourceFunction<MarketingUserBehavior> {

        // 控制是否正常运行的标识位
        private Boolean running = true;

        // 定义用户行为和渠道的范围
        private List<String> behaviorList = Arrays.asList("CLICK", "DOWNLOAD", "INSTALL", "UNINSTALL");
        private List<String> channelList = Arrays.asList("app store", "wechat", "weibo");

        private Random random = new Random();

        @Override
        public void run(SourceContext<MarketingUserBehavior> sourceContext) throws Exception {
            while (this.running) {
                Long id = this.random.nextLong();
                String behavior = this.behaviorList.get(random.nextInt(behaviorList.size()));
                String channel = this.channelList.get(random.nextInt(this.channelList.size()));
                Long timestamp = System.currentTimeMillis();

                // 发出数据
                sourceContext.collect(new MarketingUserBehavior(id, behavior, channel, timestamp));

                Thread.sleep(100L);
            }
        }

        @Override
        public void cancel() {
            this.running = false;
        }
    }

    // 自定义增量聚合函数
    static class MarketingCountAgg implements AggregateFunction<MarketingUserBehavior, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(MarketingUserBehavior marketingUserBehavior, Long accumulator) {
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

    static class MarketingCountResult extends ProcessWindowFunction<Long, ChannelPromotionCount, Tuple, TimeWindow> {
        @Override
        public void process(Tuple tuple, ProcessWindowFunction<Long, ChannelPromotionCount, Tuple, TimeWindow>.Context context, Iterable<Long> iterable, Collector<ChannelPromotionCount> collector) throws Exception {
            String channel = tuple.getField(0);
            String behavior = tuple.getField(1);
            String windowEnd = new Timestamp(context.window().getEnd()).toString();
            Long count = iterable.iterator().next();
            collector.collect(new ChannelPromotionCount(channel, behavior, windowEnd, count));
        }
    }
}
