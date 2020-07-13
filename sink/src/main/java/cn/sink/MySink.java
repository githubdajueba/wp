package cn.sink;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**]
 * 自定义sink
 *打成jar包上传至flume安装目录中的lib目录下
 *
 *
 *修改配置文件
 *
 * mysink.properties:
 * a1.sources  =  r1
 * a1.sinks  =  k1
 * a1.channels  =  c1
 *
 * a1.sources.r1.type  =  http
 * a1.sources.r1.bind  =  0.0.0.0
 * a1.sources.r1.port  =  22222
 *
 * a1.sinks.k1.type  =  cn.tedu.flume.MySink
 *
 * a1.channels.c1.type  =  memory
 * a1.channels.c1.capacity  =  1000
 * a1.channels.c1.transactionCapacity  =  100
 *
 * a1.sources.r1.channels  =  c1
 * a1.sinks.k1.channel  =  c1
 *
 *
 * 启动:
 * ../bin/flume-ng agent -c ./ -f ./mysink.properties -n a1 -Dflume.root.logger=INFO,console
 *
 *
 */
public class MySink extends AbstractSink implements Configurable {
    private static final Logger LOG = LoggerFactory.getLogger(MySink.class);

    public Status process() throws EventDeliveryException {
        Status stat;
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        Event event;
        transaction.begin();

        while(true){
            event = channel.take();
            if(event != null){
                break;
            }
        }

        try {
            LOG.info(new String(event.getBody()));
            LOG.info("输出在这里");
            transaction.commit();
            stat = Status.READY;
        } catch (Exception e) {
            e.printStackTrace();
            transaction.rollback();
            stat = Status.BACKOFF;
        } finally {
            transaction.close();
        }

        LOG.info(stat.toString());
        return stat;
    }
    private String myProp;

    public void configure(Context context) {
        String myProp = context.getString("myProp", "defaultValue");
        this.myProp = myProp;

    }
    @Override
    public void start() {
        // Initialize the connection to the external repository (e.g. HDFS) that
        // this Sink will forward Events to ..
    }

    @Override
    public void stop () {
        // Disconnect from the external respository and do any
        // additional cleanup (e.g. releasing resources or nulling-out
        // field values) ..
    }


}
