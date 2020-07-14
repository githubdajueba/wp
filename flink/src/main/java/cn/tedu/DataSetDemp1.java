package cn.tedu;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.kafka.common.metrics.stats.Sum;

public class DataSetDemp1 {
    public static void main(String[] args) throws Exception {
        // 1. 获取执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2. 获取数据原
        DataSource<String> source = env.fromElements("hadoop", "flink", "hive", "kafka", "flink");

        // 3. 转化数据
        /**
         *  1.hadoop --> ("hadoop",1)   flink -->("flink",1) ....
         *  Tuple 元组(类似map)
         * 2.通过key分组,分别统计value值
         */
        /**
         *  map 读一个 输出一个 ,其中是执行逻辑
         *  String,  输入类型
         *  Tuple2<String,Integer> 输出类型,导flink的包
         *  Tuple2 表示传2个参数
         *  Tuple 一共25个类型 ,还可以自定义
         */
        source.map(new MapFunction<String, Tuple2<String,Integer>>() {


            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return new Tuple2<>(value,1); //("hadoop",1)
            }
        })
              // 按下标0位置元素分组,下标1位置元素求和
                .groupBy(0).sum(1)
                // 4.输出数据
                .print();
        // TODO
        // 5.提交执行
    }
}
