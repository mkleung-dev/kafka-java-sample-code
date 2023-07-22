
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Producer {

    public static void main(String[] args) {
        String bootstrapServer = "127.0.0.1:9092";
        String topic = "demo_topic";

        int threadCnt = 3;
        ProducerThread thread[];
        thread = new ProducerThread[threadCnt];
        for (int i = 0; i < threadCnt; i++) {
            thread[i] = new ProducerThread(bootstrapServer, topic, String.format("p_%05d", i));
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
