package kt.producer.demo;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerDemo {
	public static void main(String[] args) {
		System.out.println("hello");
		String bootSrapServer = "127.0.0.1:9092";
		// create Producer Properties
		Properties props = new Properties();
		props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootSrapServer);
		props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// Create Producer
		KafkaProducer<String, String> KafkaProducer = new KafkaProducer<String, String>(props);

		// Send data
		ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "hello world");

		KafkaProducer.send(record);
		KafkaProducer.flush();
		KafkaProducer.close();

	}
}
