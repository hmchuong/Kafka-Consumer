import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.bson.Document;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by hmchuong on 27/06/2017.
 */
public class TransformNode implements Runnable{
    protected KafkaConsumer consumer;
    protected KafkaProducer producer;
    String timezone_topic, main_topic, prefix_out_topic;
    protected final AtomicBoolean shutdown;
    protected final CountDownLatch shutdownLatch;
    protected Map<String,Integer> timeZones;

    // For testing
    protected ConsumerRecord lastRecord = null;
    protected ConsumerRecord firstRecord = null;
    protected boolean checkAtLeastOnce = false;

    public  TransformNode(){
        this.shutdown = new AtomicBoolean(false);
        this.shutdownLatch = new CountDownLatch(1);
        timeZones = new HashMap<>();
    }

    public TransformNode(String[] argv){
        this();
        setUp(argv);
    }

    public void setUp(String argv[]){
        Args args = new Args();
        args.buildArgument(argv);
        // Create consumer
        Properties config = new Properties();
        config.put("bootstrap.servers",args.kafka_host);
        config.put("group.id",args.group_id);
        config.put("enable.auto.commit", "false");
        config.put("heartbeat.interval.ms", "2000");
        config.put("session.timeout.ms", "6001");
        config.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        config.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer(config);

        // Create producer
        Properties proConfig = new Properties();
        proConfig.put("bootstrap.servers",args.kafka_host);
        proConfig.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        proConfig.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        proConfig.put("acks", "all");
        producer = new KafkaProducer(proConfig);

        this.timezone_topic = args.timezone_topic;
        this.main_topic = args.main_topic;
        this.prefix_out_topic = args.prefix_out_topic;

        // Load timezone from DB first
        loadTimeZones(args.db_host,args.db_name, args.collection);
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

    /** Load timezone from MongoDB
     * @param db_host   address of DB
     * @param db_name   database name
     * @param db_collection collection of timezone
     */
    private void loadTimeZones(String db_host, String db_name, String db_collection) {
        System.out.println("Loading TimeZone data from database");
        try {
            MongoClient mongo = new MongoClient(db_host);

            MongoDatabase db = mongo.getDatabase(db_name);
            MongoCollection<Document> table = db.getCollection(db_collection);
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
        }catch (MongoTimeoutException e){
            System.out.println("Cannot connect to DB at " + db_host);
            return;
        }

        System.out.println("Loading successfully, received "+timeZones.size()+" documents");
    }

    /** Commit offsets synchronously (at least once)
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
        if (firstRecord == null){
            firstRecord = record;
        }
        lastRecord = record;
        String topic = record.topic();
        String json = (String) record.value();
        System.out.println("Received topic: "+topic);
        if (topic.equals(main_topic)){
            // Process main event
            processEvent(json);
        }else{
            // Process timezone event
            updateTimeZone(json);
        }
        System.out.println();
    }

    /** Process Event event
     * @param json signup json data
     */
    private void processEvent(String json){
        System.out.println("Process main event");
        Event event = new Event(json);
        // Mapping project_id with timezone
        event.timeZone = timeZones.get(event.project_id);
        if (event.timeZone == null){
            System.out.println("Not found timezone data");
            return;
        }
        System.out.println("After mapping: "+ event.toJson());

        // Send to output topic
        sendToTopic(event);
    }

    /** Send output to another topic
     * @param event - transformed event
     */
    private void sendToTopic(Event event){

        final ProducerRecord record= new ProducerRecord<>(event.getTopic(this.prefix_out_topic),event.toJson());
            producer.send(record, (recordMetadata, e) -> {
                if (e!= null){
                    System.out.println("\nSend failed for record: "+record.toString()+"\n");
                }else{
                    System.out.println("Send successfully to "+recordMetadata.topic()+" at partition "+String.valueOf(recordMetadata.partition())+"\n");
                }
            });
    }

    /** Update timezone data
     * @param json timezone json
     */
    protected void updateTimeZone(String json){
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
        firstRecord = null;
        List<String> topics = new ArrayList<>();
        topics.add(main_topic);
        topics.add(timezone_topic);
        System.out.print("Receive message from topics "+ topics.toString());
        try {
            consumer.subscribe(topics);//, new TransformConsumerRebalanceListener(consumer));
            while (!shutdown.get()) {
                ConsumerRecords records = consumer.poll(500);

                records.forEach(record -> processMessage((ConsumerRecord)record));
                if (checkAtLeastOnce){
                    break;
                }
                doCommitSync();
            }
        }catch (WakeupException e){
        }finally {
            consumer.close();
            shutdownLatch.countDown();
        }
    }

    public void shutdown() throws InterruptedException {
        System.out.println("Shutdown");
        shutdown.set(true);
        shutdownLatch.await();
    }

    /**
     * Class for JCommander
     */
    private class Args{
        @Parameter(names = {"-kafHost","-kh"}, description = "bootstrap.servers: host of kafka.")
        private String kafka_host = "localhost:9092";

        @Parameter(names = {"-groupId","-gi"},description = "Group Id of consumer", required = true)
        private String group_id;

        @Parameter(names = {"-timezoneTopic", "-tt"},description = "Timezone topic for consumer.", required = true)
        private String timezone_topic;

        @Parameter(names = {"-eventTopic","-et"},description = "Event topic for consumer.", required = true)
        private String main_topic;

        @Parameter(names = {"-prefixOutTopic","-pot"},description = "Prefix of output topic to publish after process event.", required = true)
        private String prefix_out_topic;

        @Parameter(names = {"-dbHost","-dh"},description = "Host of MongoDB storing TimeZone.")
        private String db_host = "localhost:27017";

        @Parameter(names = {"-dbName","-dn"},description = "Name of database storing TimeZone.", required = true)
        private String db_name;

        @Parameter(names = {"-collection","-c"},description = "Collection storing TimeZone.", required = true)
        private String collection;

        @Parameter(names = {"--help","--h"}, help = true)
        private boolean help;

        private void buildArgument(String[] argv){
            JCommander jcommander = JCommander.newBuilder().build();
            jcommander.addObject(this);
            try {
                jcommander.parse(argv);
            }catch (ParameterException e){
                System.out.println(e.getMessage());
                e.getJCommander().usage();
                System.exit(1);
            }
            if (this.help){
                jcommander.usage();
                System.exit(1);
            }
        }
    }



    public static void main(String[] argv){
        //TransformNode.sampleData();

        (new TransformNode(argv)).run();
    }
}
