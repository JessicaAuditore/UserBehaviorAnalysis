package com.limitofsoul.detect;

import com.limitofsoul.beans.OrderEvent;
import com.limitofsoul.beans.OrderResult;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;
import java.util.Objects;

// 检测创建了但超过15分钟没有支付、创建但超时支付、只支付未创建的订单(cep实现)
public class OrderPayTimeoutWithCep {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<OrderEvent> inputStream = env.readTextFile(Objects.requireNonNull(OrderPayTimeoutWithCep.class.getResource("/OrderLog.csv")).getPath())
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

        // 定义带时间限制的模式
        Pattern<OrderEvent, OrderEvent> orderPayPattern = Pattern.<OrderEvent>begin("create").where(new SimpleCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent orderEvent) throws Exception {
                return "create".equals(orderEvent.getEventType());
            }
        }).followedBy("pay").where(new SimpleCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent orderEvent) throws Exception {
                return "pay".equals(orderEvent.getEventType());
            }
        }).within(Time.minutes(15));

        // 定义测输出流标签
        OutputTag<OrderResult> orderTimeoutTag = new OutputTag<OrderResult>("order-timeout"){};

        // 将pattern应用到数据流上，得到pattern stream
        PatternStream<OrderEvent> patternStream = CEP.pattern(inputStream.keyBy(OrderEvent::getOrderId), orderPayPattern);

        // 调用select实现对匹配复杂时间和超时复杂时间的提取和处理
        SingleOutputStreamOperator<OrderResult> resultStream = patternStream.select(orderTimeoutTag, new OrderTimeoutSelect(), new OrderPaySelect());

        resultStream.getSideOutput(orderTimeoutTag).print();
        env.execute();
    }

    static class OrderTimeoutSelect implements PatternTimeoutFunction<OrderEvent, OrderResult> {
        @Override
        public OrderResult timeout(Map<String, List<OrderEvent>> map, long timeoutTimestamp) throws Exception {
            // timeoutTimestamp为超时时间戳
            OrderEvent orderEvent = map.get("create").iterator().next();
            return new OrderResult(orderEvent.getOrderId(), "timeout " + timeoutTimestamp);
        }
    }

    static class OrderPaySelect implements PatternSelectFunction<OrderEvent, OrderResult> {
        @Override
        public OrderResult select(Map<String, List<OrderEvent>> map) throws Exception {
            Long payedOrderId = map.get("pay").iterator().next().getOrderId();
            return new OrderResult(payedOrderId, "payed");
        }
    }
}
