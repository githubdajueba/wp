package cn.tedu;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;

/**
 * 针对DataSet 的source
 * fromElements 从E对象中读取数据
 * fromCollection 从C对象中读取数据
 * readTextFile 从文件中读取数据(支持从分布式文件系统中读取)
 *        readTextFile("hdfs://hadoop01:9000/dong/data.txt")
 */
public class DataSetSource {
    public static void main(String[] args) throws Exception {
        // 1.获取执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //2.获取数据源
//        List<Integer> list = new ArrayList<>();
//        list.add(1);
//        list.add(2);
//        list.add(3);
//        list.add(4);
//
//        DataSource<Integer> source = env.fromCollection(list);
/**
 *
 // read text file from local files system
 DataSet<String> localLines = env.readTextFile(
 "file:///path/to/my/textfile");

 // read text file from an HDFS running at nnHost:nnPort
 DataSet<String> hdfsLines = env.readTextFile(
 "hdfs://nnHost:nnPort/path/to/my/textfile");

 */
        DataSource<String> source = env.readTextFile(
                // file:///E:\eclipse\wp\flink\data.txt

                // file:E:\eclipse\wp\flink\src\main\resources\data.txt
                "data.txt");
        //3.转化数据
        //4.输出数据
        source.print();
        //5.触发执行

    }
}
