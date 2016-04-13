import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

/**
 * @author Chih Wen Tai This class as its name is a consumer of Kafka running in
 *         a thread.
 */
public class KafkaSimpleConsumer extends Thread {

	public KafkaSimpleConsumer() {
	}

	public static void main(String args[]) {
		KafkaSimpleConsumer consumer = new KafkaSimpleConsumer();
		consumer.start();
	}

	/**
	 * You need number of attributes for connecting to Kafka server as a message consumer.
	 * The attributes are stored in a properties. This properties will be used by the KafkaConsumer 
	 * to connect with Kafka server. A topic name should be given in the subscribe method for scribing
	 * topic once you have connected with the server.
	 * */
	public void run() {
		// Mode mode = Mode.valueOf(args[0]);

		Properties props = new Properties();
		// Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group1");
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
		props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
		props.put(ConsumerConfig.SESSION_TIMEOUT_MS, "30000");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY, "roundrobin");

		@SuppressWarnings("resource")
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		String topic = "newtopic";

		TopicPartition partition = new TopicPartition(topic, 0);
		consumer.subscribe(partition);

		while (true) {
			Map<java.lang.String, ConsumerRecords<String, String>> map = consumer.poll(1000);
			if (map != null)

				synchronized (Thread.currentThread()) {
					try {
						Thread.currentThread().wait(2000);
					} catch (InterruptedException e) {
					}
				}
		}
	}

}
