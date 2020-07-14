package cn.tedu;

import akka.dispatch.Filter;
import akka.dispatch.Foreach;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;

import java.util.Collection;

/**
 *  map: 一个输入,一个结果
 *  flatMap: 输入一个,多个结果
 *  reduce: 计算
 *  filter: 过滤
 *  reduceGroup:
 *  distinct: 去重
 *  join: 可以多表查询 外键关联,左外 右外
 */
public class DataSetTransformation {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<Integer> source1 =
                env.fromElements(1,2,3,4,5);

        DataSource<String> source2 = env.fromElements("007_liupx_female");
    // map
        source1.map(new MapFunction<Integer, String>() {

            @Override
            public String map(Integer integer) throws Exception {
                return integer.toString();
            }
        }).print();
    // flatMap
         source2.flatMap(new FlatMapFunction<String,
                 String>() {
             @Override
             public void flatMap(String s, Collector<String> collector) throws Exception {
                 String[] split = s.split("_");
                 for (String v : split){
                     collector.collect(v);
                 }
             }
         }).print();
        System.out.println("===========");
        DataSource<String> source3 = env.fromElements(
                "007_liupx_female_bbbb",
                "008_liqi_male_baodiii",
                "009_yiyi_female_bingbing",
                "006_yuyy_male_nnnn");
         //   filter
        source3.map(new MapFunction<String, Tuple4<String,String,String,String>>() {

            @Override
            public Tuple4<String, String, String, String> map(String s) throws Exception {
                String[] s1 = s.split("_");

                return new Tuple4<>(s1[0],s1[1],s1[2],s1[3]);
            }
        }).filter(new FilterFunction<Tuple4<String, String, String, String>>() {
            @Override
            public boolean filter(Tuple4<String, String, String, String> s) throws Exception {
                // 返回值结果为true 表示过滤后将此输出
                // 获得"male"的结果
                return s.f2.equals("male");
            }
        }).print();

    // reduce
        DataSource<Integer> source4 = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8);
        source4.reduce(new ReduceFunction<Integer>() {
            @Override
            public Integer reduce(Integer integer, Integer t1) throws Exception {
                System.out.println(integer + t1);
                return integer+t1;
            }
        }).print();
     // reduceGroup
        DataSource<Integer> source5 = env.fromElements(1,2,3,4,5);
        source5.reduceGroup(new GroupReduceFunction<Integer, Integer>() {
            @Override
            public void reduce(Iterable<Integer> iterable, Collector<Integer> collector) throws Exception {
               int sum = 0;
                for ( Integer value: iterable
                     ) {

                   sum += value;
                   collector.collect(sum);
                }
            }
        }).print();

     //
        DataSource source6 =  env.fromElements(
                new Tuple2<String,Integer>("dondcc",1),
                new Tuple2<String,Integer>("dongcc2",1));
        source6.sum(1).print();

     // distinct
        DataSource source7 =  env.fromElements(1,2,3,1,2,3,4);
        source7.distinct().print();
     // join
        DataSource source8 =  env.fromElements(
                "1|chenzishu|南",
                "1|其类|女","3|liupeizia|male"
        );
        DataSource source9 =  env.fromElements(
                "1|ceo","2|engineer","3|proseor"
        );
        MapOperator map1 = source8.map(new MapFunction<String, Tuple3<String, String, String>>() {

            @Override
            public Tuple3<String, String, String> map(String s) throws Exception {
                String[] strs = s.split("\\|");

                return new Tuple3<>(strs[0], strs[1], strs[2]);
            }
        });
        MapOperator map2 = source9.map(new MapFunction<String, Tuple2<String, String>>() {


            @Override
            public Tuple2<String, String> map(String s) throws Exception {
                String[] strs = s.split("\\|");

                return new Tuple2<>(strs[0], strs[1]);
            }
        });
        map1.join(map2).where(0).equalTo(0)
                .projectFirst(0,1,2)
                .projectSecond(1).print();
    }
}
