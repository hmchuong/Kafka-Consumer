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
    // Properties
    protected KafkaConsumer consumer;
    protected KafkaProducer producer;
    protected String timezoneTopic, untransformedTopic, prefixOfOutTopic;
    protected final AtomicBoolean shutdown;
    protected final CountDownLatch shutdownLatch;
    protected Map<String,Integer> timeZones;

    // For testing
    protected ConsumerRecord lastMessage = null;
    protected ConsumerRecord firstMessage = null;
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

    /** Setup node with arguments list
     * @param argv arguments list
     */
    public void setUp(String argv[]){
        ArgumentParser argumentParser = new ArgumentParser();
        argumentParser.buildArgument(argv);

        // Create consumer
        Properties config = new Properties();
        config.put("bootstrap.servers", argumentParser.kafkaHost);
        config.put("group.id", argumentParser.groupId);
        config.put("enable.auto.commit", "false");
        config.put("heartbeat.interval.ms", "2000");
        config.put("session.timeout.ms", "6001");
        config.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        config.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer(config);

        // Create producer
        Properties proConfig = new Properties();
        proConfig.put("bootstrap.servers", argumentParser.kafkaHost);
        proConfig.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        proConfig.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        proConfig.put("acks", "all");
        producer = new KafkaProducer(proConfig);

        // Topic configs
        this.timezoneTopic = argumentParser.timezoneTopic;
        this.untransformedTopic = argumentParser.mainTopic;
        this.prefixOfOutTopic = argumentParser.prefixOfOutTopic;

        // Load timezones from DB first
        loadTimeZones(argumentParser.dbHost, argumentParser.dbName, argumentParser.collection);
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
     * @param dbHost   address of DB
     * @param dbName   database name
     * @param dbCollection collection of timezone
     */
    private void loadTimeZones(String dbHost, String dbName, String dbCollection) {
        System.out.println("Loading TimeZone data from database");
        try {
            MongoClient mongo = new MongoClient(dbHost);

            MongoDatabase db = mongo.getDatabase(dbName);
            MongoCollection<Document> table = db.getCollection(dbCollection);
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
            System.out.println("Cannot connect to DB at " + dbHost);
            return;
        }

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

    /** Process received message
     * @param message message to process
     */
    private void processMessage(ConsumerRecord message){
        // Update firstMessage and lastMessage
        if (firstMessage == null){
            firstMessage = message;
        }
        lastMessage = message;

        String topic = message.topic();
        String messageValue = (String) message.value();

        if (topic.equals(untransformedTopic)){
            // Process transform event
            processTransformEvent(messageValue);
        }else{
            // Process timezone update event
            updateTimeZone(messageValue);
        }
        System.out.println();
    }

    /** Process transform event
     * @param json signup json data
     */
    private void processTransformEvent(String json){
        System.out.println("Process transform event");
        Event event = new Event(json);

        // Mapping project_id with timezone
        event.setTimeZone(timeZones.get(event.getProjectId()));
        if (event.getTimeZone() == null){
            System.out.println("Not found timezone data");
            return;
        }
        System.out.println("After mapping: "+ event.toString());

        // Send to output topic
        sendToOutputTopic(event);
    }

    /** Send transformed event to another topic
     * @param event - transformed event
     */
    private void sendToOutputTopic(Event event){

        final ProducerRecord record= new ProducerRecord<>(event.getOutTopic(this.prefixOfOutTopic),event.toString());
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
            String projectId = JsonPath.read(json, "$.project_id");
            Integer timezone = JsonPath.read(json, "$.timezone");
            timeZones.put(projectId, timezone);
            System.out.println("Updated "+projectId+" with timezone "+timezone);
        }catch (PathNotFoundException e){
            e.printStackTrace();
            System.out.println("Update timezone failed");
        }
    }

    @Override
    public void run() {
        // Delete firstMessage when starting
        firstMessage = null;

        // Init a list of topics
        List<String> topics = new ArrayList<>();
        topics.add(untransformedTopic);
        topics.add(timezoneTopic);
        System.out.print("Receive message from topics "+ topics.toString());
        try {
            // Subscribe topics to receive message
            consumer.subscribe(topics);

            while (shutdown.get() == false) {
                ConsumerRecords records = consumer.poll(500);

                records.forEach(record -> processMessage((ConsumerRecord)record));

                // To interrupt before commit --> Testing at-least-once scenario
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

    /** Shutdown consumer
     * @throws InterruptedException
     */
    public void shutdown() throws InterruptedException {
        System.out.println("Shutdown");
        shutdown.set(true);
        shutdownLatch.await();
    }

    /**
     * Class for JCommander
     */
    private class ArgumentParser {
        @Parameter(names = {"-kafHost","-kh"}, description = "bootstrap.servers: host of kafka.")
        private String kafkaHost = "localhost:9092";

        @Parameter(names = {"-groupId","-gi"},description = "Group Id of consumer", required = true)
        private String groupId;

        @Parameter(names = {"-timezoneTopic", "-tt"},description = "Timezone topic for consumer.", required = true)
        private String timezoneTopic;

        @Parameter(names = {"-eventTopic","-et"},description = "Event topic for consumer.", required = true)
        private String mainTopic;

        @Parameter(names = {"-prefixOfOutTopic","-pot"},description = "Prefix of output topic to publish after process event.", required = true)
        private String prefixOfOutTopic;

        @Parameter(names = {"-dbHost","-dh"},description = "Host of MongoDB storing TimeZone.")
        private String dbHost = "localhost:27017";

        @Parameter(names = {"-dbName","-dn"},description = "Name of database storing TimeZone.", required = true)
        private String dbName;

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

            // Show helper
            if (this.help){
                jcommander.usage();
                System.exit(1);
            }
        }
    }



    public static void main(String[] argv){
        (new TransformNode(argv)).run();
    }
}