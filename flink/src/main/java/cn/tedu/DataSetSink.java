package cn.tedu;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;

import java.util.ArrayList;
import java.util.List;

/**
 *  针对 DataSet的 sink
 *  从 collection中读取数据,并将数据输出到本地文件
 */
public class DataSetSink {
    public static void main(String[] args) throws Exception {
        /**
         * https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/batch/
         *  writeAsText() /
         *   print() /
         */
        // 1.获取执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //2.加载/创建初始数据
        List<Integer> list = new ArrayList<>();
        list.add(1);
        list.add(2);
        list.add(3);
        list.add(4);
        DataSource<Integer> source = env.fromCollection(list);
        //3.指定对此数据的转换

        //4.指定将计算结果放在何处
        source.writeAsText("result.txt");
        //5.触发程序执行
        env.execute("DataSetSink");

    }
}
