import java.util.Properties;
import java.util.Arrays;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.json.simple.JSONObject;

public class Consumer {
  public static void main(String[] args) throws Exception {
    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:2181");
    props.put("group.id", "consumer");

    KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
    consumer.subscribe(Arrays.asList("regular", "special"));

    while (true) {
      ConsumerRecords<String, String> records = consumer.poll(1000);
      if (records.count() == 0) {
        // Wait for 1000 ms. If no records are put into the buffer, turn off the consumer.
        System.out.print("No records received.");
        consumer.close();
      }
      // We get the records
      JSONParser parser = new JSONParser();
      for (ConsumerRecord<String, String> record : records) {
        String topic = record.topic();
        JSONObject json = parser.parse(record.value());
        String type = json.getString("type");

        // A better way: each consumer only subscribes to one topic, so that maintainance is easier.
        switch(topic) {
          case "regular":
            if (type == "regular1") {
              // process message with type regular1
            } else if (type == "regular2") {
              // process message with type regular2
            } else {
              throw new IllegalArgumentException("No such type: " + type);
            }
            break;
          case "special":
            // process special message
            break;
          default:
            throw new IllegalStateException("No such topic: " + topic);
        }
      }
    }
  }
}