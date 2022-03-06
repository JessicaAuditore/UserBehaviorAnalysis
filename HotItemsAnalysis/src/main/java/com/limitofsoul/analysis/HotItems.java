package com.limitofsoul.analysis;

import com.limitofsoul.beans.ItemViewCount;
import com.limitofsoul.beans.UserBehavior;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Properties;

// 每5分钟统计一次1小时内的热门商品
public class HotItems {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

//        DataStream<String> inputStream = env.readTextFile("HotItemsAnalysis/src/main/resources/UserBehavior.csv");
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.50.16:9091,192.168.50.16:9092,192.168.50.16:9093");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");
        DataStream<String> inputStream = env.addSource(new FlinkKafkaConsumer<String>("hotItems", new SimpleStringSchema(), properties));

        DataStream<UserBehavior> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return  new UserBehavior(new Long(fields[0]), new Long(fields[1]), new Integer(fields[2]), fields[3], new Long(fields[4]));
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
            @Override
            public long extractAscendingTimestamp(UserBehavior userBehavior) {
                return userBehavior.getTimestamp() * 1000L;
            }
        });

        // 过滤分组开窗聚合，得到每个窗口内各个商品的count
        DataStream<ItemViewCount> windowAggStream = dataStream.filter(data -> "pv".equals(data.getBehavior()))
                .keyBy("itemId")
                .timeWindow(Time.hours(1), Time.minutes(5))
                .aggregate(new ItemCountAgg(), new WindowItemCountResult()); // 每次新来的数据增量 最后关窗的时候全量

        // 收集同一窗口的所有商品count数据，排序输出top n
        DataStream<String> resultStream = windowAggStream
                .keyBy("windowEnd")
                .process(new TopNHotItems(5));

        resultStream.print();
        env.execute();
    }

    // 自定义增量聚合函数
    static class ItemCountAgg implements AggregateFunction<UserBehavior, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior userBehavior, Long accumulator) {
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

    // 自定义全窗口函数
    static class WindowItemCountResult implements WindowFunction<Long, ItemViewCount, Tuple, TimeWindow> {
        @Override
        public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Long> iterable, Collector<ItemViewCount> collector) throws Exception {
            Long itemId = tuple.getField(0);
            Long windowEnd = timeWindow.getEnd();
            Long count = iterable.iterator().next();
            collector.collect(new ItemViewCount(itemId, windowEnd, count));
        }
    }

    static class TopNHotItems extends KeyedProcessFunction<Tuple, ItemViewCount, String> {

        private final Integer topSize;

        public TopNHotItems(Integer topSize) {
            this.topSize = topSize;
        }

        // 定义列表状态，保存当前窗口内所有输出ItemViewCount
        ListState<ItemViewCount> itemViewCountListState;

        @Override
        public void open(Configuration parameters) throws Exception {
            this.itemViewCountListState = getRuntimeContext().getListState(new ListStateDescriptor<ItemViewCount>("item-view-count-list", ItemViewCount.class));
        }

        @Override
        public void processElement(ItemViewCount itemViewCount, KeyedProcessFunction<Tuple, ItemViewCount, String>.Context context, Collector<String> collector) throws Exception {
            // 每来一条数据 存入list 注册定时器
            this.itemViewCountListState.add(itemViewCount);
            context.timerService().registerEventTimeTimer(itemViewCount.getWindowEnd() + 1); // flink底层通过时间戳来区别定时器，同一个窗口来的数据注册的定时器相同
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Tuple, ItemViewCount, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            // 排序输出
            ArrayList<ItemViewCount> itemViewCounts = Lists.newArrayList(itemViewCountListState.get().iterator());
            itemViewCounts.sort((o1, o2) -> o2.getCount().intValue() - o1.getCount().intValue());
            StringBuilder resultBuilder = new StringBuilder();
            resultBuilder.append("=========================================\n");
            resultBuilder.append("窗口结束时间: ").append(new Timestamp(timestamp - 1)).append("\n");
            for(int i = 0;i < Math.min(this.topSize, itemViewCounts.size());i++){
                ItemViewCount currentItemViewCount = itemViewCounts.get(i);
                resultBuilder.append("No.").append(i+1).append(": ")
                        .append(" 商品ID = ").append(currentItemViewCount.getItemId())
                        .append(" 热门度 = ").append(currentItemViewCount.getCount()).append("\n");
            }
            resultBuilder.append("=========================================\n\n");
            out.collect(resultBuilder.toString());

            Thread.sleep(1000L);
        }
    }
}
