package com.limitofsoul.detect;

import com.limitofsoul.beans.OrderEvent;
import com.limitofsoul.beans.OrderResult;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Objects;

// 检测创建了但超过15分钟没有支付、创建但超时支付、只支付未创建的订单
// 根据订单id分组，每来一个创建事件，如果已经支付，则输出，否则定时15分钟，15分钟内支付事件已来则删除定时器，定时器时间到，将未支付事件输出到侧输出流，每来一个支付事件同理
public class OrderPayTimeout {

    private final static OutputTag<OrderResult> orderTimeoutTag = new OutputTag<OrderResult>("order-timeout") {
    };

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<OrderEvent> inputStream = env.readTextFile(Objects.requireNonNull(OrderPayTimeout.class.getResource("/OrderLog.csv")).getPath())
                .map(line -> {
                    String[] fields = line.split(",");
                    return new OrderEvent(new Long(fields[0]), fields[1], fields[2], new Long(fields[3]));
                })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<OrderEvent>(Time.seconds(3)) {
                    @Override
                    public long extractTimestamp(OrderEvent orderEvent) {
                        return orderEvent.getTimestamp() * 1000L;
                    }
                });

        // 主流输出正常匹配订单事件，侧输出流输出超时订单事件
        SingleOutputStreamOperator<OrderResult> resultStream = inputStream.keyBy(OrderEvent::getOrderId).process(new KeyedProcessFunction<Long, OrderEvent, OrderResult>() {
            private ValueState<Boolean> isCreatedState;
            private ValueState<Boolean> isPayedState;
            private ValueState<Long> timeTs;

            @Override
            public void open(Configuration parameters) throws Exception {
                this.isCreatedState = getRuntimeContext().getState((new ValueStateDescriptor<Boolean>("is-created", Boolean.class, false)));
                this.isPayedState = getRuntimeContext().getState((new ValueStateDescriptor<Boolean>("is-payed", Boolean.class, false)));
                this.timeTs = getRuntimeContext().getState(new ValueStateDescriptor<Long>("time", Long.class));
            }

            @Override
            public void processElement(OrderEvent orderEvent, KeyedProcessFunction<Long, OrderEvent, OrderResult>.Context context, Collector<OrderResult> collector) throws Exception {
                if ("create".equals(orderEvent.getEventType())) {
                    // 来的是create
                    this.isCreatedState.update(true);
                    if (this.isPayedState.value()) {
                        // 正常支付，输出正常匹配结果
                        collector.collect(new OrderResult(orderEvent.getOrderId(), "payed successfully"));
                        this.isCreatedState.clear();
                        this.isPayedState.clear();
                        context.timerService().deleteEventTimeTimer(this.timeTs.value());
                        this.timeTs.clear();
                    } else {
                        // 还没有支付
                        this.isPayedState.update(false);
                        this.timeTs.update((orderEvent.getTimestamp() + 15 * 60) * 1000L);
                        context.timerService().registerEventTimeTimer(this.timeTs.value());
                    }
                } else if ("pay".equals(orderEvent.getEventType())) {
                    // 来的是pay
                    this.isPayedState.update(true);
                    if (this.isCreatedState.value()) {
                        // 已经下单
                        if (orderEvent.getTimestamp() * 1000L < this.timeTs.value()) {
                            // 成功支付
                            collector.collect(new OrderResult(orderEvent.getOrderId(), "payed successfully"));
                        } else {
                            // 已经超时
                            context.output(orderTimeoutTag, new OrderResult(orderEvent.getOrderId(), "payed but already timeout"));
                        }
                        this.isCreatedState.clear();
                        this.isPayedState.clear();
                        context.timerService().deleteEventTimeTimer(this.timeTs.value());
                        this.timeTs.clear();
                    } else {
                        // 还没有下单 注册定时器，等待下单事件
                        context.timerService().registerEventTimeTimer(orderEvent.getTimestamp() * 1000L);
                        this.timeTs.update(orderEvent.getTimestamp() * 1000L);
                        this.isPayedState.update(true);
                    }
                }
            }

            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<Long, OrderEvent, OrderResult>.OnTimerContext ctx, Collector<OrderResult> out) throws Exception {
                // 判断定时器触发时类型
                if (this.isPayedState.value()) {
                    // pay来了 create没来
                    ctx.output(orderTimeoutTag, new OrderResult(ctx.getCurrentKey(), "payed but not found created log"));
                } else {
                    // create来了 pay每来
                    ctx.output(orderTimeoutTag, new OrderResult(ctx.getCurrentKey(), "timeout"));
                }
                this.isCreatedState.clear();
                this.isPayedState.clear();
                this.timeTs.clear();
            }
        });

        resultStream.getSideOutput(orderTimeoutTag).print();
        env.execute();
    }
}
