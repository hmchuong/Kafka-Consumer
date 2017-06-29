import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import com.mongodb.*;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.bson.Document;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

/**
 * Created by hmchuong on 27/06/2017.
 */
public class TransformNode implements Runnable{
    private KafkaConsumer consumer;
    String timezone_topic, main_topic;
    private final AtomicBoolean shutdown;
    private final CountDownLatch shutdownLatch;
    private final Map<String,Integer> timeZones;

    public TransformNode(Args args){
        Properties config = new Properties();
        config.put("bootstrap.servers",args.kafka_host);
        config.put("group.id",args.group_id);
        config.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        config.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer(config);
        this.timezone_topic = args.timezone_topic;
        this.main_topic = args.main_topic;
        this.shutdown = new AtomicBoolean(false);
        this.shutdownLatch = new CountDownLatch(1);
        timeZones = new HashMap<>();
        loadTimeZones(args);
    }

    /**
     * Create sample data and save to DB
     */
    final static void sampleData(){
        MongoClient mongo = new MongoClient("localhost",27017);
        MongoDatabase db = mongo.getDatabase("hasBrain");
        MongoCollection<Document> table = db.getCollection("timezone");
        for (int i = 0; i < 10; i++) {
            Document document = new Document();
            document.put("project_id", "5865e5a7fba95e82a88072b"+i);
            document.put("timezone", i*500);
            table.insertOne(document);
        }
        for (int i = 0; i < 26; i++){
            char end = (char)('a'+i);
            Document document = new Document();
            document.put("project_id", "5865e5a7fba95e82a88072b"+end);
            document.put("timezone", -i*500%12000);
            table.insertOne(document);
        }
        mongo.close();
    }

    /** Load timezone data from database
     * @param args parameters
     */
    private void loadTimeZones(Args args){
        System.out.println("Loading TimeZone data from database");
        MongoClient mongo = new MongoClient(args.db_host);
        if (mongo == null){
            System.out.println("Cannot connect to DB at " + args.db_host);
        }
        MongoDatabase db = mongo.getDatabase(args.db_name);
        MongoCollection<Document> table = db.getCollection(args.collection);
        FindIterable<Document> find = table.find();
        MongoCursor<Document> cursor = find.iterator();
        try{
            while (cursor.hasNext()){
                Document doc = cursor.next();
                String projectId = (String) doc.get("project_id");
                Integer timeZone = (Integer) doc.get("timezone");
                timeZones.put(projectId,timeZone);
            }
        }finally {
            cursor.close();
        }
        mongo.close();
        System.out.println("Loading successfully, received "+timeZones.size()+" documents");
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
        System.out.println("Received topic: "+topic);
        if (topic.equals(main_topic)){
            // Process signup event
            processSignUpEvent(json);
        }else{
            // Process timezone event
            updateTimeZone(json);
        }
        System.out.println();
    }

    /** Process SignUp event
     * @param json signup json data
     */
    private void processSignUpEvent(String json){
        System.out.println("Process main event");
        SignUp signUp = new SignUp(json);
        // Mapping project_id with timezone
        signUp.timeZone = timeZones.get(signUp.project_id);
        // Sending to another topic
        if (signUp.timeZone == null){
            System.out.println("Not found timezone data");
            return;
        }
        System.out.println("After mapping: "+signUp.toJson());
    }

    /** Update timezone data
     * @param json timezone json
     */
    private void updateTimeZone(String json){
        System.out.println("Processing timezone update event");
        try {
            String id = JsonPath.read(json, "$.project_id");
            Integer timezone = JsonPath.read(json, "$.timezone");
            timeZones.put(id, timezone);
            System.out.println("Updated "+id+" with timezone "+timezone);
        }catch (PathNotFoundException e){
            e.printStackTrace();
            System.out.println("Update timezone failed");
        }
    }

    @Override
    public void run() {
        List<String> topics = new ArrayList<>();
        topics.add(main_topic);
        topics.add(timezone_topic);
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
        @Parameter(names = {"-kafhost","-kh"}, description = "bootstrap.servers: host of kafka. Default: localhost:9092")
        private String kafka_host = "localhost:9092";

        @Parameter(names = {"-groupId","-gi"},description = "Group Id of consumer. Default: id0")
        private String group_id = "id0";

        @Parameter(names = {"-timetopic", "-tt"},description = "Timezone topic for consumer. Default: timezone")
        private String timezone_topic = "timezone";

        @Parameter(names = {"-maintopic","-mt"},description = "Main topic for consumer. Default: event")
        private String main_topic = "event";

        @Parameter(names = {"-dbhost","-dh"},description = "Host of MongoDB storing TimeZone. Default: localhost:27017")
        private String db_host = "localhost:27017";

        @Parameter(names = {"-dbname","-dn"},description = "Name of database storing TimeZone. Default: hasBrain")
        private String db_name = "hasBrain";

        @Parameter(names = {"-collection","-c"},description = "Collection storing TimeZone. Default: timezone")
        private String collection = "timezone";
    }

    public static void main(String[] argv){
        //TransformNode.sampleData();
        Args args = new Args();
        JCommander.newBuilder()
                .addObject(args)
                .build()
                .parse(argv);
        (new TransformNode(args)).run();
    }
}
