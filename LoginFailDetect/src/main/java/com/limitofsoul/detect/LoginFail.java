package com.limitofsoul.detect;

import com.limitofsoul.beans.LoginEvent;
import com.limitofsoul.beans.LoginFailWarning;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.Objects;

// 检测2秒内连续登录失败次数
// 以用户id分组，每来一个登录失败时间事件，保存并看和前一个登录失败事件的时间差是否小于2s，但不能处理乱序数据
public class LoginFail {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<LoginEvent> inputStream = env.readTextFile(Objects.requireNonNull(LoginFail.class.getResource("/LoginLog.csv")).getPath())
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

        // 自定义处理函数检测连续登录失败事件
        DataStream<LoginFailWarning> warningStream = inputStream.keyBy(LoginEvent::getUserId).process(new LoginFailDetectWarning(3));

        warningStream.print();
        env.execute();
    }

    static class LoginFailDetectWarning extends KeyedProcessFunction<Long, LoginEvent, LoginFailWarning> {
        // 连续登录失败次数
        private Integer maxFailTimes;

        public LoginFailDetectWarning(Integer maxFailTimes) {
            this.maxFailTimes = maxFailTimes;
        }

        // 保存2秒内所有的登录失败事件
        private ListState<LoginEvent> loginEventListState;

        @Override
        public void open(Configuration parameters) throws Exception {
            this.loginEventListState = getRuntimeContext().getListState(new ListStateDescriptor<LoginEvent>("login-fail-last", LoginEvent.class));
        }

        @Override
        public void processElement(LoginEvent loginEvent, KeyedProcessFunction<Long, LoginEvent, LoginFailWarning>.Context context, Collector<LoginFailWarning> collector) throws Exception {
            // 判断当前登录事件类型
            if ("fail".equals(loginEvent.getLoginState())) {
                // 登录失败
                Iterator<LoginEvent> iterator = this.loginEventListState.get().iterator();
                if (iterator.hasNext()) {
                    LoginEvent firstFailEvent = iterator.next();
                    if (Math.abs(loginEvent.getTimestamp() - firstFailEvent.getTimestamp()) <= 2) {
                        collector.collect(new LoginFailWarning(context.getCurrentKey(), firstFailEvent.getTimestamp(), loginEvent.getTimestamp(), "login fail 2 times in 2s"));
                    }
                    this.loginEventListState.clear();
                    this.loginEventListState.add(loginEvent);
                } else {
                    this.loginEventListState.add(loginEvent);
                }
            } else {
                // 登录成功
                this.loginEventListState.clear();
            }
        }
    }
}
