import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.junit.Assert;
import org.junit.Test;

/**
 * Created by hmchuong on 04/07/2017.
 */
public class TransformNodeTest extends TransformNode{
    String args[] = "-c timezone -dn hasBrain -gi test_chuong -tt timezone -et event -pot transformed --v 0".split(" ");
    final int maxTimePerMessage = 30;
    String sampleMessage = "{\"_id\" : {\"$oid\": \"5950ec7490d78f373802856d\"},\"_app_bundle_id\" : \"com.2359Media.FoxPlay.alpha\",\"_country\" : \"SG\",\"_isp\" : \"AS22363 Powerhouse Management, Inc.\",\"_os\" : \"iOS\",\"_os_version\" : \"10.2.1\",\"_account_id\" : \"5950ec6e7766605d3b9c292d\",\"_ip\" : \"43.251.165.69\",\"_sent_at\" : Date(\"2017-06-29T18:10:28.867+07:00\"),\"_type\" : \"facebook\",\"_app_version_code\" : \"1\",\"createdAt\" : Date(\"2017-06-29T18:10:28.865+07:00\"),\"_sdk_version\" : \"0.14.0\",\"_free\" : false,\"_app_version_name\" : \"1.2.59\",\"_visit_id\" : \"iLKKoRLHNT2Gs3XNopyf0Yw\",\"name\" : \"_signup\",\"project_id\" : {\"$oid\": \"5865e5a7fba95e82a88072be\"},\"profile_id\" : \"iPf4KwVKPTLeIlbeS20dvHQ\"}";

    public TransformNodeTest(){
        super.setUp(args);
    }

    /**
     * Print test name before every thing
     * @param name - name of test
     */
    void printTestName(String name){
        System.out.println();
        System.out.println("---------- Test: "+name+" ----------");
    }

    /**
     * Print passed result
     */
    void printSuccess(){
        System.out.println();
        System.out.println("--> Passed");
        System.out.println("-------------------------------------");
    }

    /** Send and receive sample message
     * @param receiver - consumer receives message
     */
    public void sendNReceiveSampleMessage(TransformNode receiver){
        receiver.lastMessage = null;

        // Begin receiving message
        System.out.println("Consumer: Begin consume message");
        Thread t1 = new Thread(receiver);
        t1.start();

        ProducerRecord record= new ProducerRecord<>("event", sampleMessage);
        Future<Metadata> futureRecord = producer.send(record);
        try {
            futureRecord.get();
            System.out.println("Producer: Send message successfully");
            for (int sec = 0; sec < maxTimePerMessage; sec++){
                if (receiver.lastMessage != null){
                    System.out.println("Consumer: Receive message after "+sec+"s");
                    break;
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            }
            receiver.shutdown();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void argumentsParser() {
        printTestName("Arguments parser");
        Assert.assertEquals("Parsing -et failed","event",super.untransformedTopic);
        Assert.assertEquals("Parsing -pot failed","transformed",super.prefixOfOutTopic);
        Assert.assertEquals("Parsing -tt failed","timezone",super.timezoneTopic);
        printSuccess();
    }

    @Test
    public void modifyTimeZones(){
        printTestName("Update timezone");
        int oldNumOfTimezones = super.timeZones.size();

        // Add one timezone
        System.out.println("1. Add 1 timezone");
        if (!timeZones.containsKey("timezone_test")){
            super.updateTimeZone("{\"project_id\":\"timezone_test\",\"timezone\":500}");
            Assert.assertTrue("Add one timezone failed",oldNumOfTimezones+1 == timeZones.size());
        }

        // Update one timezone
        System.out.println("2. Update 1 timezone");
        super.updateTimeZone("{\"project_id\":\"timezone_test\",\"timezone\":2000}");
        Assert.assertEquals("Modify one timezone failed",2000,(int)timeZones.get("timezone_test"));

        // Wrong timezone message structure
        System.out.println("3. Handle wrong structure timezone");
        super.updateTimeZone("{\"project_id\":\"timezone_test\",\"timezones\":3000}");
        Assert.assertEquals("Handle wrong structure failed",2000,(int)timeZones.get("timezone_test"));
        printSuccess();
    }

    @Test
    public void receiveRightMessage(){
        printTestName("Receive right message");
        this.checkAtLeastOnce = false;
        sendNReceiveSampleMessage(this);

        Assert.assertTrue("Cannot receive message after "+ maxTimePerMessage +"s",this.lastMessage != null);
        Assert.assertEquals("Receive wrong message",sampleMessage, this.lastMessage.value());
        printSuccess();
    }

    @Test
    public void offsetCommitting(){
        printTestName("Starting offset");
        // Send first message and receive last offset
        System.out.println("1. Send first message");
        this.checkAtLeastOnce = false;
        sendNReceiveSampleMessage(this);
        long lastOffset = this.lastMessage.offset();
        System.out.println("Last received offset: "+lastOffset);
        // Send second message and receive first offset
        System.out.println("\n2. Send second message");
        TransformNode anotherConsumer = new TransformNode(args);
        sendNReceiveSampleMessage(anotherConsumer);
        long firstOffset = anotherConsumer.firstMessage.offset();
        System.out.println("Starting offset of a new consumer: "+firstOffset);

        // Compare first and last offset
        Assert.assertTrue("Consumer starts at processed offset",firstOffset>lastOffset);
        printSuccess();
    }

    @Test
    //DisplayName("At-least-once")
    public void atLeastOnce(){
        printTestName("At-least-once");
        // Run message --> then crash before commit --> Check offset
        this.checkAtLeastOnce = true;

        // Send message and receive last offset
        System.out.println("1. Send first message and interrupt before commit");
        sendNReceiveSampleMessage(this);
        long lastOffset = super.lastMessage.offset();
        System.out.println("Last received offset: "+lastOffset);

        // Send second time and receive first offset
        System.out.println("\n2. Send second message");
        TransformNode anotherConsumer = new TransformNode(args);
        sendNReceiveSampleMessage(anotherConsumer);
        long firstOffset = anotherConsumer.firstMessage.offset();
        System.out.println("Starting offset of a new consumer: "+firstOffset);

        // Check first and last offset
        Assert.assertTrue("Lost message",firstOffset<=lastOffset);
        printSuccess();
    }

    @Test
    public void outputTopic(){
        printTestName("Output topic");
        // Prepare a test consumer for receive transformed message
        List<String> outputTopics = new ArrayList<>();
        outputTopics.add("transformed_signup");
        BaseConsumer postConsumer = new BaseConsumer("localhost:9092","test_chuong",outputTopics);

        // Start receiving message
        System.out.println("Output consumer: start consume message");
        Thread threadOfTester = new Thread(postConsumer);
        threadOfTester.start();

        // Send sample message to the consumer
        int oldNumOfRecords = postConsumer.getNumOfRecords();
        sendNReceiveSampleMessage(this);

        // Waiting for receiving output message
        for (int sec = 0; sec < maxTimePerMessage; sec++){
            if (oldNumOfRecords < postConsumer.getNumOfRecords()){
                System.out.println("Output consumer: Receive message after "+sec+"s");
                break;
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }
        }
        try {
            postConsumer.shutdown();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // Check whether record is received
        Assert.assertTrue("Cannot receive transformed message",oldNumOfRecords < postConsumer.getNumOfRecords());
        printSuccess();
    }
}