package cn.tedu;

import cn.tedu.po.Log;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

// 总结：Flink、addSource清洗keyBy分区sum合并，print统一输出

public class LogStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties pros = new Properties();
        pros.setProperty("bootstrap.servers","hadoop01:9092");
        pros.setProperty("group.id","text");
        DataStreamSource<String> source = env.addSource(
                new FlinkKafkaConsumer<>("flux",
                new SimpleStringSchema(), pros));
        source.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {

                return !s.equals(" ")
                        &&s.split("\\|").length==16
                        &&s.split("\\|")[14].split("_").length==3;
            }
        }).map(new MapFunction<String, Log>() {
            @Override
            public Log map(String s) throws Exception {
                Log log = new Log();
                log.setCount(1l);
                log.setUrlName(s.split("\\|")[1]);
              // ...log.set...
                return log;
            }

//            @Override
//            public String map(String s) throws Exception {
//                return "=="+s;
//            }
        }).keyBy("urlName")
                .timeWindow(Time.seconds(5))
                .sum("count")

                .print().setParallelism(1);
//
        env.execute("LogStream");


    }
}
