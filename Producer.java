import java.util.Properties;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Producer {
  public static void main(String[] args) throws Exception{
    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:2181");
    props.put("batch.size", "2048");
    props.put("linger.ms", "10");
    KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

    for (int i = 0; i < 1000; ++i) {
      if (i % 100 == 0) {
        producer.send(new ProducerRecord<String, String>(
          "special",
          String.format("{\"type\":\"special\", \"key\":%d}", i)));
      } else if (i % 20 == 0) {
        producer.send(new ProducerRecord<String, String>(
          "regular",
          String.format("{\"type\":\"regular1\", \"key\":%d}", i)));
      } else {
        producer.send(new ProducerRecord<String, String>(
          "regular",
          String.format("{\"type\":\"regular2\", \"key\":%d}", i)));
      }
    }
  }
}