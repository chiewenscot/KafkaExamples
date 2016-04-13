import java.io.Closeable;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * @author Chih Wen Tai
 * This class was created and called by KafkaSimpleProducerThread.
 * */
public class KafkaSimpleProducer<K,V> implements Closeable {

	private KafkaProducer<K, V> producer;
	
	public KafkaSimpleProducer() {
	}
	
	@SuppressWarnings("rawtypes")
	public KafkaSimpleProducer createProducer(Properties config){
		producer = new KafkaProducer<K, V>(config);
		return this;
	}
	
	public void asyncPublish(String topic, K key, V value){
		producer.send(new ProducerRecord<K,V>(topic, key, value));
	}
	
	public RecordMetadata syncPublish(String topic, K key, V value) throws InterruptedException, ExecutionException{
		return producer.send(new ProducerRecord<K,V>(topic, key, value)).get();
	}
	
	@Override
	public void close(){
		producer.close();
	}

	/**
	 * This main method starts a thread for publishing example message to Kafka server.
	 * 
	 * @param	args	no value needed to give for running this program
	 * */
	public static void main(String[] args) throws InterruptedException, ExecutionException {
		KafkaSimpleProducerThread thread = new KafkaSimpleProducerThread();
		thread.start();
	}

}
