package com.limitofsoul.analysis;

import com.limitofsoul.beans.PageViewCount;
import com.limitofsoul.beans.UserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.util.Objects;

// 统计1小时内网站独立访客数uv 布隆过滤器去重(防止set内存占用过大)
// 防止窗口内访客数过大，可以使用布隆过滤器(redis中的setbit实现，窗口关闭时间作为key)，将每个用户作为布隆过滤器中的一位来判断用户是否访问过，每来一条数据就访问一下布隆过滤器(getbit)
public class UvWithBloomFilter {

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
                .trigger(new MyTrigger()) // 时间窗口默认关闭时触发一次操作 可以自定义触发时间 如来一个数据触发一次
                .process(new UvCountResultWithBloomFilter());

        uvStream.print();
        env.execute();
    }

    static class MyTrigger extends Trigger<UserBehavior, TimeWindow> {
        @Override
        public TriggerResult onElement(UserBehavior userBehavior, long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            // 每一条数据到来，直接触发窗口计算，并清空窗口
            return TriggerResult.FIRE_AND_PURGE;
        }

        @Override
        public TriggerResult onProcessingTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {

        }
    }

    // 自定义一个布隆过滤器
    static class MyBloomFilter {
        // 定义位图的大小，一般定义为2的整次幂
        private Integer cap;

        public MyBloomFilter(Integer cap) {
            this.cap = cap;
        }

        // 实现hash函数
        public Long hashCode(String value, Integer seed) {
            long result = 0L;
            for (int i = 0; i < value.length(); i++) {
                result = result * seed + value.charAt(i);
            }
            return result & (cap - 1); // %操作简化
        }
    }

    // 使用布隆过滤器判断每个窗户中的用户是否重复出现，原先用set，但随着用户量的增加内存占用过大，布隆过滤器中一个用户只占1bit，减少占用量
    // 另外所有uvCount保存在redis中
    static class UvCountResultWithBloomFilter extends ProcessAllWindowFunction<UserBehavior, PageViewCount, TimeWindow> {

        // 定义jedis和布隆过滤器
        private Jedis jedis;
        private MyBloomFilter myBloomFilter;

        @Override
        public void open(Configuration parameters) throws Exception {
            this.jedis = new Jedis("101.43.147.253", 6379);
            this.myBloomFilter = new MyBloomFilter(1 << 29); // 2的29次方(bit)=64MB
        }

        @Override
        public void close() throws Exception {
            this.jedis.close();
        }

        @Override
        public void process(ProcessAllWindowFunction<UserBehavior, PageViewCount, TimeWindow>.Context context, Iterable<UserBehavior> iterable, Collector<PageViewCount> collector) throws Exception {
            // 将位图和窗口count值全部存入redis，用windowEnd作为key
            String bitmapKey = String.valueOf(context.window().getEnd());
            // 把count值存成一张表
            String countHashName = "uv_count";
            String countKey = bitmapKey;

            Long userId = iterable.iterator().next().getUserId();
            Long offset = this.myBloomFilter.hashCode(userId.toString(), 61);
            // 用redis的getbit命令，判断对应位置的值
            Boolean isExist = this.jedis.getbit(bitmapKey, offset);
            if (!isExist) {
                // 如果不存在，对应位置置1
                jedis.setbit(bitmapKey, offset, true);
                // 更新count
                long uvCount = 0L; // 初始count值
                String uvCountString = jedis.hget(countHashName, countKey);
                if (!"".equals(uvCountString)) {
                    uvCount = Long.parseLong(uvCountString);
                }
                jedis.hset(countHashName, countKey, String.valueOf(uvCount + 1));

                collector.collect(new PageViewCount("uv", context.window().getEnd(), uvCount + 1));
            }
        }
    }
}
