package com.limitofsoul.analysis;

import com.limitofsoul.beans.ApacheLogEvent;
import com.limitofsoul.beans.PageViewCount;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

// 每5秒统计一次10分钟以内的热门页面
// 过滤非get请求和资源文件，按url分组开窗，得到每个窗口内每个url请求的count，再按窗口分组，定时器定到窗口关闭的时间，保存每个窗口内所有url的count状态，排序输出
public class HotPages {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        DataStream<String> inputStream = env.readTextFile(Objects.requireNonNull(HotPages.class.getResource("/apache.log")).getPath());

        DataStream<ApacheLogEvent> dataStream = inputStream.map(line -> {
            String[] fields = line.split(" ");
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
            Long timestamp = simpleDateFormat.parse(fields[3]).getTime();
            return new ApacheLogEvent(fields[0], fields[1], timestamp, fields[5], fields[6]);
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ApacheLogEvent>(Time.seconds(1)) {
            @Override
            public long extractTimestamp(ApacheLogEvent apacheLogEvent) {
                return apacheLogEvent.getTimestamp();
            }
        });

        // 分组开窗聚合
        OutputTag<ApacheLogEvent> lateTag = new OutputTag<ApacheLogEvent>("late"){};
        SingleOutputStreamOperator<PageViewCount> windowAggStream = dataStream.filter(data -> "GET".equals(data.getMethod()))
                .filter(data -> { // 过滤资源文件的请求
                    String regex = "^((?!\\.(css|js|png|ico)$).)*$";
                    return Pattern.matches(regex, data.getUrl());
                })
                .keyBy(ApacheLogEvent::getUrl) // 按照url分组
                .timeWindow(Time.minutes(10), Time.seconds(5))
                .allowedLateness(Time.minutes(1)) // 允许迟到数据 1秒时先输出一个结果，到1分钟前，每来一个数据就输出一个结果(触发一次WindowFunction)
                .sideOutputLateData(lateTag)  // 1分钟之后来的数据只有没有任何一个窗口能接收，才会输出到侧数据流
                .aggregate(new PageCountAgg(), new PageCountResult());
        windowAggStream.getSideOutput(lateTag).print("late");

        // 收集同一窗口count数量，排序输出
        DataStream<String> resultStream = windowAggStream.keyBy(PageViewCount::getWindowEnd)
                .process(new TopNHotPages(3));

        resultStream.print();
        env.execute();
    }

    // 自定义预聚合函数
    static class PageCountAgg implements AggregateFunction<ApacheLogEvent, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(ApacheLogEvent apacheLogEvent, Long accumulator) {
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

    static class PageCountResult implements WindowFunction<Long, PageViewCount, String, TimeWindow> {
        @Override
        public void apply(String url, TimeWindow timeWindow, Iterable<Long> iterable, Collector<PageViewCount> collector) throws Exception {
            collector.collect(new PageViewCount(url, timeWindow.getEnd(), iterable.iterator().next()));
        }
    }

    static class TopNHotPages extends KeyedProcessFunction<Long, PageViewCount, String> {

        private final Integer topSize;

        public TopNHotPages(Integer topSize) {
            this.topSize = topSize;
        }

        // 定义状态，保存当前所有PageViewCount 一个状态对应一个key，同一个key的状态相同
//        private ListState<PageViewCount> pageViewCountListState;
        private MapState<String, Long> pageViewCountMapState;

        @Override
        public void open(Configuration parameters) throws Exception {
            this.pageViewCountMapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Long>("page-count-map", String.class, Long.class));
        }

        @Override
        public void processElement(PageViewCount pageViewCount, KeyedProcessFunction<Long, PageViewCount, String>.Context context, Collector<String> collector) throws Exception {
            this.pageViewCountMapState.put(pageViewCount.getUrl(), pageViewCount.getCount());
            context.timerService().registerEventTimeTimer(pageViewCount.getWindowEnd() + 1);

            // 注册1分钟之后的定时器 用于清空状态
            context.timerService().registerEventTimeTimer(pageViewCount.getWindowEnd() + 60 * 1000L);
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Long, PageViewCount, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            // 判断是否到了窗口关闭清理时间
            if (timestamp == ctx.getCurrentKey() + 60 * 1000L) {
                this.pageViewCountMapState.clear();
                return;
            }

            ArrayList<Map.Entry<String, Long>> pageViewCount = Lists.newArrayList(this.pageViewCountMapState.entries().iterator());
            pageViewCount.sort((o1, o2) -> o2.getValue().compareTo(o1.getValue()));
            StringBuilder resultBuilder = new StringBuilder();
            resultBuilder.append("=========================================\n");
            resultBuilder.append("窗口结束时间: ").append(new Timestamp(timestamp - 1)).append("\n");
            for(int i = 0;i < Math.min(this.topSize, pageViewCount.size());i++){
                Map.Entry<String, Long> currentPageViewCount = pageViewCount.get(i);
                resultBuilder.append("No.").append(i+1).append(": ")
                        .append(" 页面URL = ").append(currentPageViewCount.getKey())
                        .append(" 浏览量 = ").append(currentPageViewCount.getValue()).append("\n");
            }
            resultBuilder.append("=========================================\n\n");
            out.collect(resultBuilder.toString());

            Thread.sleep(1000L);
        }
    }
}
