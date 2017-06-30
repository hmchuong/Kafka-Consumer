import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;

/**
 * Created by hmchuong on 30/06/2017.
 */
public class TransformConsumerRebalanceListener implements ConsumerRebalanceListener {
    private OffsetManager offsetManager = new OffsetManager("transform");
    private Consumer<String,String> consumer;

    public TransformConsumerRebalanceListener(Consumer<String, String> consumer){
        this.consumer = consumer;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> collection) {
        for (TopicPartition partition: collection){
            offsetManager.saveOffsetInExternalStore(partition.topic(),partition.partition(),consumer.position(partition));
        }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> collection) {
        for (TopicPartition partition: collection){
            consumer.seek(partition, offsetManager.readOffsetFromExternalStore(partition.topic(),partition.partition()));
        }
    }
}
