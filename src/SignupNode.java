import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by hmchuong on 29/06/2017.
 */
public class SignupNode implements Runnable{
    private KafkaConsumer consumer;
    String topic;
    private final AtomicBoolean shutdown;
    private final CountDownLatch shutdownLatch;

    public SignupNode(SignupNode.Args args){
        // Create consumer
        Properties config = new Properties();
        config.put("bootstrap.servers",args.kafka_host);
        config.put("group.id",args.group_id);

        config.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        config.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer(config);

        this.topic = args.topic;
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

    /** Process received record
     * @param record record to process
     */
    private void processMessage(ConsumerRecord record){
        String topic = record.topic();
        String json = (String) record.value();
        processEvent(json);
    }

    /** Process event
     * @param json signup json data
     */
    private void processEvent(String json){
        System.out.println("Process main event");
        Event event = new Event(json);
        System.out.println("ProjectID: "+ event.project_id);
        System.out.println("TimeZone: "+ event.timeZone);
    }

    @Override
    public void run() {
        List<String> topics = new ArrayList<>();
        topics.add(topic);
        System.out.print("Receive message from topics "+ topics.toString());
        try {
            consumer.subscribe(topics);
            while (!shutdown.get()) {
                ConsumerRecords records = consumer.poll(500);

                records.forEach(record -> processMessage((ConsumerRecord)record));

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

    /**
     * Class for JCommander
     */
    public static class Args{
        @Parameter(names = {"-kafHost","-kh"}, description = "bootstrap.servers: host of kafka. Default: localhost:9092")
        private String kafka_host = "localhost:9092";

        @Parameter(names = {"-groupId","-gi"},description = "Group Id of consumer. Default: id0")
        private String group_id = "id0";

        @Parameter(names = {"-topic", "-t"},description = "Topic for consumer. Default: signup")
        private String topic = "signup";
    }

    public static void main(String[] argv){
        SignupNode.Args args = new SignupNode.Args();
        JCommander.newBuilder()
                .addObject(args)
                .build()
                .parse(argv);
        (new SignupNode(args)).run();
    }
}
