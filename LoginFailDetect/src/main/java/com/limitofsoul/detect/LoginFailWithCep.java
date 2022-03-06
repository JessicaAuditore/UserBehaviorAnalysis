package com.limitofsoul.detect;

import com.limitofsoul.beans.LoginEvent;
import com.limitofsoul.beans.LoginFailWarning;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;
import java.util.Objects;

// 检测2秒内连续登录失败次数 cep 可以解决由于乱序数据导致的复杂事件
public class LoginFailWithCep {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<LoginEvent> inputStream = env.readTextFile(Objects.requireNonNull(LoginFailWithCep.class.getResource("/LoginLog.csv")).getPath())
                .map(line -> {
                    String[] fields = line.split(",");
                    return new LoginEvent(new Long(fields[0]), fields[1], fields[2], Long.valueOf(fields[3]));
                })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LoginEvent>(Time.seconds(3)) {
                    @Override
                    public long extractTimestamp(LoginEvent loginEvent) {
                        return loginEvent.getTimestamp() * 1000L;
                    }
                });

        // 定义一个匹配模式
        // 3秒内连续三次失败
        Pattern<LoginEvent, LoginEvent> loginFailPattern = Pattern.<LoginEvent>begin("failEvents").where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent) throws Exception {
                        return "fail".equals(loginEvent.getLoginState());
                    }
                }).times(3)
                .consecutive() // 连续
                .within(Time.seconds(3)); // [,)

        // 将匹配模式应用到数据流上，得到一个pattern stream
        PatternStream<LoginEvent> patternStream = CEP.pattern(inputStream.keyBy(LoginEvent::getUserId), loginFailPattern);

        // 检出符合条件的复杂事件，进行转换处理，得到报警信息
        DataStream<LoginFailWarning> warningStream = patternStream.select(new PatternSelectFunction<LoginEvent, LoginFailWarning>() {
            @Override
            public LoginFailWarning select(Map<String, List<LoginEvent>> map) throws Exception {
                LoginEvent firstFailEvent = map.get("failEvents").get(0);
                LoginEvent lastFailEvent = map.get("failEvents").get(map.get("failEvents").size() - 1);
                return new LoginFailWarning(firstFailEvent.getUserId(), firstFailEvent.getTimestamp(), lastFailEvent.getTimestamp(), "login fail for " + map.get("failEvents").size()
                        + " times in " + (lastFailEvent.getTimestamp() - firstFailEvent.getTimestamp() + 1) + "s");
            }
        });

        warningStream.print();
        env.execute();
    }
}
