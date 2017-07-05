import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Created by hmchuong on 04/07/2017.
 */
class TransformNodeTest extends TransformNode{
    String args[] = "-c timezone -dn hasBrain -gi test_chuong -tt timezone -et event -pot transformed".split(" ");

    public TransformNodeTest(){
        super.setUp(args);
    }

    @BeforeEach
    void setUp() {
        Thread t1 = new Thread(this);
        t1.start();
    }

    @AfterEach
    void tearDown() {
        try {
            this.shutdown();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    @DisplayName("Arguments parser")
    void argumentsParser() {
        assertEquals("event",super.main_topic);
        assertEquals("transformed",super.prefix_out_topic);
        assertEquals("timezone",super.timezone_topic);
    }

    @Test
    @DisplayName("Update timezone")
    void modifyTimeZones(){
        int lengthBefore = super.timeZones.size();

        // Add one timezone
        if (!timeZones.containsKey("timezone_test")){
            super.updateTimeZone("{\"project_id\":\"timezone_test\",\"timezone\":500}");
            assertTrue(lengthBefore+1 == timeZones.size(),"Add one timezone failed");
        }

        // Modify one timezone
        super.updateTimeZone("{\"project_id\":\"timezone_test\",\"timezone\":2000}");
        assertEquals(2000,(int)timeZones.get("timezone_test"),"Modify one timezone failed");

        // Wrong message structure
        super.updateTimeZone("{\"project_id\":\"timezone_test\",\"timezones\":3000}");
        assertEquals(2000,(int)timeZones.get("timezone_test"),"Handle wrong structure failed");
    }

    @Test
    @DisplayName("Receive right message")
    void receiveRightMessage(){
        final int maxTime = 30;
        String recordValue = "{\"_id\" : {\"$oid\": \"5950ec7490d78f373802856d\"},\"_app_bundle_id\" : \"com.2359Media.FoxPlay.alpha\",\"_country\" : \"SG\",\"_isp\" : \"AS22363 Powerhouse Management, Inc.\",\"_os\" : \"iOS\",\"_os_version\" : \"10.2.1\",\"_account_id\" : \"5950ec6e7766605d3b9c292d\",\"_ip\" : \"43.251.165.69\",\"_sent_at\" : Date(\"2017-06-29T18:10:28.867+07:00\"),\"_type\" : \"facebook\",\"_app_version_code\" : \"1\",\"createdAt\" : Date(\"2017-06-29T18:10:28.865+07:00\"),\"_sdk_version\" : \"0.14.0\",\"_free\" : false,\"_app_version_name\" : \"1.2.59\",\"_visit_id\" : \"iLKKoRLHNT2Gs3XNopyf0Yw\",\"name\" : \"_signup\",\"project_id\" : {\"$oid\": \"5865e5a7fba95e82a88072be\"},\"profile_id\" : \"iPf4KwVKPTLeIlbeS20dvHQ\"}";
        super.lastRecord = null;

        ProducerRecord record= new ProducerRecord<>("event",recordValue);
        Future<Metadata> futureRecord = producer.send(record);
        try {
            futureRecord.get();
            for (int i = 0; i < maxTime; i++){
                if (super.lastRecord != null){
                    System.out.println("Received message after "+i+"s");
                    break;
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            }
            assertTrue(super.lastRecord != null,"Cannot receive message after "+maxTime+"s");
            assertEquals(recordValue, super.lastRecord.value(),"Receive wrong message");
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    @Test
    @DisplayName("start offset")
    void offsetCommitting(){
        // Send first message and receive last offset
        this.checkAtLeastOnce = false;
        receiveRightMessage();
        long lastOffset = super.lastRecord.offset();

        // Shutdown consumer
        try {
            shutdown();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // Send second time and receive first offset
        long firstOffset = sendSampleRecord(new TransformNode(args),0);

        // Compare first and last offset
        System.out.println("Last received offset: "+lastOffset);
        System.out.println("Start offset once a new consumer start: "+firstOffset);
        assertTrue(firstOffset>lastOffset,"Consumer starts at processed offset");
    }

    long sendSampleRecord(TransformNode node, int firstOrLast){
        node.checkAtLeastOnce = false;
        final int maxTime = 30;
        String recordValue = "{\"_id\" : {\"$oid\": \"5950ec7490d78f373802856d\"},\"_app_bundle_id\" : \"com.2359Media.FoxPlay.alpha\",\"_country\" : \"SG\",\"_isp\" : \"AS22363 Powerhouse Management, Inc.\",\"_os\" : \"iOS\",\"_os_version\" : \"10.2.1\",\"_account_id\" : \"5950ec6e7766605d3b9c292d\",\"_ip\" : \"43.251.165.69\",\"_sent_at\" : Date(\"2017-06-29T18:10:28.867+07:00\"),\"_type\" : \"facebook\",\"_app_version_code\" : \"1\",\"createdAt\" : Date(\"2017-06-29T18:10:28.865+07:00\"),\"_sdk_version\" : \"0.14.0\",\"_free\" : false,\"_app_version_name\" : \"1.2.59\",\"_visit_id\" : \"iLKKoRLHNT2Gs3XNopyf0Yw\",\"name\" : \"_signup\",\"project_id\" : {\"$oid\": \"5865e5a7fba95e82a88072be\"},\"profile_id\" : \"iPf4KwVKPTLeIlbeS20dvHQ\"}";
        node.lastRecord = null;

        Thread t1 = new Thread(node);
        t1.start();

        ProducerRecord record= new ProducerRecord<>("event",recordValue);
        Future<Metadata> futureRecord = producer.send(record);
        try {
            futureRecord.get();
            for (int i = 0; i < maxTime; i++){
                if (node.lastRecord != null){
                    System.out.println("Received message after "+i+"s");
                    break;
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            }
            node.shutdown();
            assertTrue(node.lastRecord != null,"Cannot receive message after "+maxTime+"s");
            assertEquals(recordValue, node.lastRecord.value(),"Receive wrong message");
            if (firstOrLast == 0){
                return node.firstRecord.offset();
            }else{
                return node.lastRecord.offset();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return 0;
    }

    @Test
    @DisplayName("At-least-once")
    void atLeastOnce(){
        // Run message --> then crash before commit --> Check offset
        super.checkAtLeastOnce = true;

        // Send message and receive last offset
        receiveRightMessage();
        long lastOffset = super.lastRecord.offset();
        try {
            shutdown();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // Send second time and receive first offset
        long firstOffset = sendSampleRecord(new TransformNode(args),0);

        // Check first and last offset
        System.out.println("Last received offset: "+lastOffset);
        System.out.println("Start offset once a new consumer start: "+firstOffset);
        assertTrue(firstOffset<=lastOffset,"Lost message");
    }

    @Test
    @DisplayName("Output topic")
    void outputTopic(){

    }
}