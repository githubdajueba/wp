package cn.tedu;

import cn.tedu.po.User;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class DataStreamDemo {
    public static void main(String[] args) throws Exception {

        //
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> source = env.fromElements(
                1, 2, 3, 4, 5,6,7,8,9,10);
      //  env.socketTextStream("hostname",port)
        source.print()
                // 设置并行/允许内核数
                .setParallelism(2);
        /**
         * 输出显示: 左边: 电脑内核(数)
         *        右侧: 结果
         */
        env.execute("DataStreamDemo");

        /**
         *  1|董长春|18|男
         *  2|刘玉江|10|男
         *  3|王鑫|20|女
         *  4|大大|21|男
         *  5|绿绿|20|女
         */
      //  List<User> list = new ArrayList<>();
//        list.add("1|董长春|18|男");
//        list.add("2|刘玉江|10|男");
//        list.add("3|王鑫|20|女");
//        list.add("4|大大|21|男");
//        list.add("5|绿绿|20|女");

        DataStreamSource<String> source1 = env.fromElements(
                "1|董长春|18|男",
                "2|刘玉江|10|男",
                "3|王鑫|20|女",
                "4|大大|21|男",
                "5|绿绿|20|女");
        source1.map(new MapFunction<String, User>() {
            @Override
            public User map(String s) throws Exception {
                String[] strs = s.split("\\|");
                User user = new User();
                user.setId(strs[0]);
                user.setName(strs[1]);
                user.setAge(strs[2]);
                user.setGender(strs[3]);
                user.setCount(1);
                return user;
            }
        }).keyBy("gender").sum("count").print();

        env.execute("keyby");
    }
}
