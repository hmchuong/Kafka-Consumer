import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by hmchuong on 29/06/2017.
 */
public class BaseConsumer implements Runnable{
    private KafkaConsumer consumer;
    private List<String> topics;
    private final AtomicBoolean shutdown;
    private final CountDownLatch shutdownLatch;
    private int numOfRecords = 0;

    public int getNumOfRecords() {
        return numOfRecords;
    }

    public BaseConsumer(String server, String groupid, List<String> topics){
        // Create consumer
        Properties config = new Properties();
        config.put("bootstrap.servers",server);
        config.put("group.id",groupid);
        config.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        config.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer(config);

        this.topics = topics;
        this.shutdown = new AtomicBoolean(false);
        this.shutdownLatch = new CountDownLatch(1);
    }

    /** Commit offsets synchronously
     * @return successful or not
     */
    private boolean doCommitSync() {
        try {
            consumer.commitSync();
            return true;
        } catch (CommitFailedException e) {
            System.out.println("Commit failed "+ e);
            return false;
        }
    }

    @Override
    public void run() {
        System.out.print("Receive message from topics "+ topics.toString());
        try {
            consumer.subscribe(topics);
            while (!shutdown.get()) {
                ConsumerRecords records = consumer.poll(500);

                records.forEach(record -> numOfRecords++);

                doCommitSync();
            }
        }catch (WakeupException e){
        }finally {
            consumer.close();
            shutdownLatch.countDown();
        }
    }

    public void shutdown() throws InterruptedException {
        shutdown.set(true);
        shutdownLatch.await();
    }
}