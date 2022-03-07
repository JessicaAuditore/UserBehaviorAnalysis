package com.limitofsoul.detect;

import com.limitofsoul.beans.OrderEvent;
import com.limitofsoul.beans.ReceiveEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import scala.Tuple2;

import java.util.Objects;

// 判断pay事件和支付事件是否匹配
public class TxPayMatch {

    private final static OutputTag<OrderEvent> unmatchedPays = new OutputTag<OrderEvent>("unmatched-pays") {
    };
    private final static OutputTag<ReceiveEvent> unmatchedReceipts = new OutputTag<ReceiveEvent>("unmatched-receipts") {
    };

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<OrderEvent> orderEventStream = env.readTextFile(Objects.requireNonNull(TxPayMatch.class.getResource("/OrderLog.csv")).getPath())
                .map(line -> {
                    String[] fields = line.split(",");
                    return new OrderEvent(new Long(fields[0]), fields[1], fields[2], new Long(fields[3]));
                }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderEvent>() {
                    @Override
                    public long extractAscendingTimestamp(OrderEvent orderEvent) {
                        return orderEvent.getTimestamp() * 1000L;
                    }
                }).filter(data -> !"".equals(data.getTxId())); // 只取pay事件

        DataStream<ReceiveEvent> receiveEventStream = env.readTextFile(Objects.requireNonNull(TxPayMatch.class.getResource("/ReceiptLog.csv")).getPath())
                .map(line -> {
                    String[] fields = line.split(",");
                    return new ReceiveEvent(fields[0], fields[1], new Long(fields[2]));
                }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<ReceiveEvent>() {
                    @Override
                    public long extractAscendingTimestamp(ReceiveEvent receiveEvent) {
                        return receiveEvent.getTimestamp() * 1000L;
                    }
                });

        // 合流匹配处理，不匹配的事件输出侧输出流
        SingleOutputStreamOperator<Tuple2<OrderEvent, ReceiveEvent>> resultStream = orderEventStream.connect(receiveEventStream)
                .keyBy(OrderEvent::getTxId, ReceiveEvent::getTxId)
                .process(new CoProcessFunction<OrderEvent, ReceiveEvent, Tuple2<OrderEvent, ReceiveEvent>>() {
                    // 保存当前已经到来的订单支付事件和到账事件
                    private ValueState<OrderEvent> payState;
                    private ValueState<ReceiveEvent> receiveState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        this.payState = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>("pay", OrderEvent.class));
                        this.receiveState = getRuntimeContext().getState(new ValueStateDescriptor<ReceiveEvent>("receive", ReceiveEvent.class));
                    }

                    @Override
                    public void processElement1(OrderEvent payEvent, CoProcessFunction<OrderEvent, ReceiveEvent, Tuple2<OrderEvent, ReceiveEvent>>.Context context, Collector<Tuple2<OrderEvent, ReceiveEvent>> collector) throws Exception {
                        // pay事件来了，判断是否有对应的到账事件
                        ReceiveEvent receiveEvent = this.receiveState.value();
                        if (receiveEvent != null) {
                            collector.collect(new Tuple2<>(payEvent, receiveEvent));
                            this.payState.clear();
                            this.receiveState.clear();
                        } else {
                            context.timerService().registerEventTimeTimer((payEvent.getTimestamp() + 5) * 1000L); // pay事件最多等receive事件5秒
                            this.payState.update(payEvent);
                        }
                    }

                    @Override
                    public void processElement2(ReceiveEvent receiveEvent, CoProcessFunction<OrderEvent, ReceiveEvent, Tuple2<OrderEvent, ReceiveEvent>>.Context context, Collector<Tuple2<OrderEvent, ReceiveEvent>> collector) throws Exception {
                        // receive事件来了，判断是否有对应的pay事件
                        OrderEvent payEvent = this.payState.value();
                        if (payEvent != null) {
                            collector.collect(new Tuple2<>(payEvent, receiveEvent));
                            this.payState.clear();
                            this.receiveState.clear();
                        } else {
                            context.timerService().registerEventTimeTimer((receiveEvent.getTimestamp() + 5) * 1000L); // receive事件最多等pay事件5秒
                            this.receiveState.update(receiveEvent);
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, CoProcessFunction<OrderEvent, ReceiveEvent, Tuple2<OrderEvent, ReceiveEvent>>.OnTimerContext ctx, Collector<Tuple2<OrderEvent, ReceiveEvent>> out) throws Exception {
                        // 判断payState和receiveState哪个为空
                        if (this.payState.value() != null) {
                            ctx.output(unmatchedPays, this.payState.value());
                        }
                        if (this.receiveState.value() != null) {
                            ctx.output(unmatchedReceipts, this.receiveState.value());
                        }
                        this.payState.clear();
                        this.receiveState.clear();
                    }
                });

        resultStream.print();
        resultStream.getSideOutput(unmatchedPays).print("unmatched-pays");
        resultStream.getSideOutput(unmatchedReceipts).print("unmatched-receipts");
        env.execute();
    }
}
