package com.limitofsoul.detect;

import com.limitofsoul.beans.OrderEvent;
import com.limitofsoul.beans.ReceiveEvent;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import scala.Tuple2;

import java.util.Objects;

// 判断pay事件和支付事件是否匹配 join实现
// pay事件流通过id和receive事件流intervalJoin，定义可以早来或晚到，但只能接收到两个事件都匹配的情况
public class TxPayMatchByJoin {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<OrderEvent> orderEventStream = env.readTextFile(Objects.requireNonNull(TxPayMatchByJoin.class.getResource("/OrderLog.csv")).getPath())
                .map(line -> {
                    String[] fields = line.split(",");
                    return new OrderEvent(new Long(fields[0]), fields[1], fields[2], new Long(fields[3]));
                }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderEvent>() {
                    @Override
                    public long extractAscendingTimestamp(OrderEvent orderEvent) {
                        return orderEvent.getTimestamp() * 1000L;
                    }
                }).filter(data -> !"".equals(data.getTxId())); // 只取pay事件

        DataStream<ReceiveEvent> receiveEventStream = env.readTextFile(Objects.requireNonNull(TxPayMatchByJoin.class.getResource("/ReceiptLog.csv")).getPath())
                .map(line -> {
                    String[] fields = line.split(",");
                    return new ReceiveEvent(fields[0], fields[1], new Long(fields[2]));
                }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<ReceiveEvent>() {
                    @Override
                    public long extractAscendingTimestamp(ReceiveEvent receiveEvent) {
                        return receiveEvent.getTimestamp() * 1000L;
                    }
                });

        // 区间连接两条流，得到匹配数据
        SingleOutputStreamOperator<Tuple2<OrderEvent, ReceiveEvent>> resultStream = orderEventStream.keyBy(OrderEvent::getTxId)
                .intervalJoin(receiveEventStream.keyBy(ReceiveEvent::getTxId))
                .between(Time.seconds(-3), Time.seconds(5)) // [-3, 5] 得到接收在order事件来之前3秒内和来之后5秒内的receive事件
                .process(new ProcessJoinFunction<OrderEvent, ReceiveEvent, Tuple2<OrderEvent, ReceiveEvent>>() {
                    // 只能接收到两个事件都匹配的情况
                    @Override
                    public void processElement(OrderEvent orderEvent, ReceiveEvent receiveEvent, ProcessJoinFunction<OrderEvent, ReceiveEvent, Tuple2<OrderEvent, ReceiveEvent>>.Context context, Collector<Tuple2<OrderEvent, ReceiveEvent>> collector) throws Exception {
                        collector.collect(new Tuple2<>(orderEvent, receiveEvent));
                    }
                });

        resultStream.print();
        env.execute();
    }
}
