package cn.conf;

/**
 *  在Hadoop配置文件中添加
 */
public class Hadoop {
    /**
     *
     *在Hadoop配置文件中加入以下内容
     *
     * hdfs-site.xml:
     *
     * <property>
     * <name>dfs.webhdfs.enabled</name>
     * <value>true</value>
     * </property>
     *
     * core-site.xml:
     *
     * <property>
     * <name>hadoop.proxyuser.root.hosts</name>
     * <value>*</value>
     * </property>
     * <property>
     * <name>hadoop.proxyuser.root.groups</name>
     * <value>*</value>
     * </property>
     *
     *启动hiveserver2
     *
     * 在hive安装目录下的bin目录中执行
     *
     *      ./hiveserver2
     *
     */
}
