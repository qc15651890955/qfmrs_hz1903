import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Properties;

public class CollectLog10 {
    public static void main(String[] args) {
        Properties prop = new Properties();
        prop.put("bootstrap.servers", "192.168.110.111:9092,192.168.110.112:9092,192.168.110.113:9092");
        prop.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(prop);
        try {
            BufferedReader bf = new BufferedReader(new FileReader(new File("D:\\cmcc.json")));
            String line = null;
            while ((line = bf.readLine()) != null) {
                Thread.sleep(1000);
                producer.send(new ProducerRecord<String, String>("hz1903b",line));
            }
            bf.close();
            producer.close();
            System.out.println("已经发送完毕");
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
}
