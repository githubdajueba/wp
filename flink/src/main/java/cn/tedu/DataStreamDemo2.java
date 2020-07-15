package cn.tedu;

import cn.tedu.po.Word;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

// https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/stream/operators/index.html

public class DataStreamDemo2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.fromElements(
                "Operators DataStream Specification of available streaming DataStream operators ");
        SingleOutputStreamOperator<Word> map = source.flatMap(new FlatMapFunction<String, Word>() {

            @Override
            public void flatMap(String s, Collector<Word> collector) throws Exception {
                String[] words = s.split(" ");
                for (String v : words) {
                    Word word = new Word();
                    word.setWord(v);
                    word.setCount(1);
                    collector.collect(word);
                }

            }
        });
        //System.out.println(map.getPreferredResources());
       // System.out.println("================");

        map.keyBy("word").reduce(new ReduceFunction<Word>() {
            @Override
            public Word reduce(Word word, Word t1) throws Exception {
                word.setCount(word.getCount()+t1.getCount());
                return word;
            }
        }).print();
        env.execute("DataStreamDemo2");
    }
}
