import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerThread extends Thread {
    private String bootstrapServer;
    private String prefix;
    private String topic;

    public ProducerThread(String bootstrapServer, String topic, String prefix) {
        this.bootstrapServer = bootstrapServer;
        this.topic = topic;
        this.prefix = prefix;
    }
    public void run() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        String key;
        String value;
        for (int i = 0; i < 1000000; i++) {
            key = String.format("%s_%07d", this.prefix, i);
            value = String.valueOf(i);
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>("ttt", key,  value);

            producer.send(producerRecord);
            System.out.println("Thread:" + getId() + ",Key:" + producerRecord.key() + ",Val:" + producerRecord.value());
        }
        producer.flush();
        producer.close();
    }
}
