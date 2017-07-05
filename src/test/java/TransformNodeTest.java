import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Created by hmchuong on 04/07/2017.
 */
class TransformNodeTest extends TransformNode{
    String args[] = "-c timezone -dn hasBrain -gi test_chuong -tt timezone -et event -pot transformed".split(" ");
    final int maxTimePerMessage = 30;
    String sampleMessage = "{\"_id\" : {\"$oid\": \"5950ec7490d78f373802856d\"},\"_app_bundle_id\" : \"com.2359Media.FoxPlay.alpha\",\"_country\" : \"SG\",\"_isp\" : \"AS22363 Powerhouse Management, Inc.\",\"_os\" : \"iOS\",\"_os_version\" : \"10.2.1\",\"_account_id\" : \"5950ec6e7766605d3b9c292d\",\"_ip\" : \"43.251.165.69\",\"_sent_at\" : Date(\"2017-06-29T18:10:28.867+07:00\"),\"_type\" : \"facebook\",\"_app_version_code\" : \"1\",\"createdAt\" : Date(\"2017-06-29T18:10:28.865+07:00\"),\"_sdk_version\" : \"0.14.0\",\"_free\" : false,\"_app_version_name\" : \"1.2.59\",\"_visit_id\" : \"iLKKoRLHNT2Gs3XNopyf0Yw\",\"name\" : \"_signup\",\"project_id\" : {\"$oid\": \"5865e5a7fba95e82a88072be\"},\"profile_id\" : \"iPf4KwVKPTLeIlbeS20dvHQ\"}";

    public TransformNodeTest(){
        super.setUp(args);
    }

    /** Send and receive sample message
     * @param receiver - consumer receives message
     */
    void sendNReceiveSampleMessage(TransformNode receiver){
        receiver.lastMessage = null;

        // Begin receiving message
        Thread t1 = new Thread(receiver);
        t1.start();

        ProducerRecord record= new ProducerRecord<>("event", sampleMessage);
        Future<Metadata> futureRecord = producer.send(record);
        try {
            futureRecord.get();
            for (int sec = 0; sec < maxTimePerMessage; sec++){
                if (receiver.lastMessage != null){
                    System.out.println("Received message after "+sec+"s");
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
    @DisplayName("Arguments parser")
    void argumentsParser() {
        assertEquals("event",super.untransformedTopic);
        assertEquals("transformed",super.prefixOfOutTopic);
        assertEquals("timezone",super.timezoneTopic);
    }

    @Test
    @DisplayName("Update timezone")
    void modifyTimeZones(){
        int oldNumOfTimezones = super.timeZones.size();

        // Add one timezone
        if (!timeZones.containsKey("timezone_test")){
            super.updateTimeZone("{\"project_id\":\"timezone_test\",\"timezone\":500}");
            assertTrue(oldNumOfTimezones+1 == timeZones.size(),"Add one timezone failed");
        }

        // Modify one timezone
        super.updateTimeZone("{\"project_id\":\"timezone_test\",\"timezone\":2000}");
        assertEquals(2000,(int)timeZones.get("timezone_test"),"Modify one timezone failed");

        // Wrong timezone message structure
        super.updateTimeZone("{\"project_id\":\"timezone_test\",\"timezones\":3000}");
        assertEquals(2000,(int)timeZones.get("timezone_test"),"Handle wrong structure failed");
    }

    @Test
    @DisplayName("Receive right message")
    void receiveRightMessage(){
        this.checkAtLeastOnce = false;
        sendNReceiveSampleMessage(this);

        assertTrue(this.lastMessage != null,"Cannot receive message after "+ maxTimePerMessage +"s");
        assertEquals(sampleMessage, this.lastMessage.value(),"Receive wrong message");
    }

    @Test
    @DisplayName("Starting offset")
    void offsetCommitting(){
        // Send first message and receive last offset
        this.checkAtLeastOnce = false;
        sendNReceiveSampleMessage(this);
        long lastOffset = this.lastMessage.offset();

        // Send second message and receive first offset
        TransformNode anotherConsumer = new TransformNode(args);
        sendNReceiveSampleMessage(anotherConsumer);
        long firstOffset = anotherConsumer.firstMessage.offset();

        // Compare first and last offset
        System.out.println("Last received offset: "+lastOffset);
        System.out.println("Starting offset of a new consumer: "+firstOffset);
        assertTrue(firstOffset>lastOffset,"Consumer starts at processed offset");
    }

    @Test
    @DisplayName("At-least-once")
    void atLeastOnce(){
        // Run message --> then crash before commit --> Check offset
        this.checkAtLeastOnce = true;

        // Send message and receive last offset
        sendNReceiveSampleMessage(this);
        long lastOffset = super.lastMessage.offset();

        // Send second time and receive first offset
        TransformNode anotherConsumer = new TransformNode(args);
        sendNReceiveSampleMessage(anotherConsumer);
        long firstOffset = anotherConsumer.firstMessage.offset();

        // Check first and last offset
        System.out.println("Last received offset: "+lastOffset);
        System.out.println("Starting offset of a new consumer: "+firstOffset);
        assertTrue(firstOffset<=lastOffset,"Lost message");
    }

    @Test
    @DisplayName("Output topic")
    void outputTopic(){
        // Prepare a test consumer for receive transformed message
        List<String> outputTopics = new ArrayList<>();
        outputTopics.add("transformed_signup");
        BaseConsumer postConsumer = new BaseConsumer("localhost:9092","test_chuong",outputTopics);

        // Start receiving message
        Thread threadOfTester = new Thread(postConsumer);
        threadOfTester.start();

        // Send sample message to the consumer
        int oldNumOfRecords = postConsumer.getNumOfRecords();
        sendNReceiveSampleMessage(this);

        // Waiting for receiving output message
        for (int sec = 0; sec < maxTimePerMessage; sec++){
            if (oldNumOfRecords < postConsumer.getNumOfRecords()){
                System.out.println("Received message after "+sec+"s");
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
        assertTrue(oldNumOfRecords < postConsumer.getNumOfRecords(),"Cannot receive transformed message");
    }
}