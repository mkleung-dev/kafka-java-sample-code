
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
public class Consumer {

    public static void main(String[] args) {
        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "group_id";
        String topic = "demo_topic";

        int threadCnt = 3;
        ConsumerThread thread[];
        thread = new ConsumerThread[threadCnt];
        for (int i = 0; i < threadCnt; i++) {
            thread[i] = new ConsumerThread(bootstrapServer, groupId, topic);
            thread[i].start();
        }
        for (int i = 0; i < threadCnt; i++) {
            synchronized (thread[i]) {
                try {
                    thread[i].wait();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
