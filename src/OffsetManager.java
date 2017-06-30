/**
 * Created by hmchuong on 30/06/2017.
 */

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The partition offset is stored in an external storage (local file system)
 */
public class OffsetManager {
    private String storagePrefix;

    public OffsetManager(String prefix){
        storagePrefix = prefix;
    }

    /**
     * Overwrite the offset for the topic in an external storage.
     * @param topic - Topic name
     * @param partition - Partition of the topic
     * @param offset - offset to be stored.
     */
    void saveOffsetInExternalStore(String topic, int partition, long offset){
        try{
            FileWriter writer = new FileWriter(storageName(topic, partition),false);
            BufferedWriter bufferedWriter = new BufferedWriter(writer);
            bufferedWriter.write(offset+"");
            bufferedWriter.flush();
            bufferedWriter.close();
        }catch(Exception e){
            e.printStackTrace();
            throw  new RuntimeException(e);
        }
    }

    long readOffsetFromExternalStore(String topic, int partition){
        try{
            Stream<String> stream = Files.lines(Paths.get(storageName(topic,partition)));
            return Long.parseLong(stream.collect(Collectors.toList()).get(0))+1;
        }catch(Exception e){
            e.printStackTrace();
        }

        return 0;
    }

    private String storageName(String topic, int partition){
        return storagePrefix + "-" + topic + "-" + partition;
    }
}
