import java.util.Properties;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * @author Chih Wen Tai
 * 
 * Apache Kafka is a publish/subscribe base messaging service.
 * This class creates a Kafka test message produce running in an independent thread. 
 * There should be a Kafka and a Zookeeper processes running before you run this program.
 * 
 * Please note that this example was using Kafka Java API for publishing message.
 * Kafka was written in Scala. In the begining Kafka only provided Scala API for programer.
 * This Java API was one of latest new Kafka API which was highly recommended from official document.
 * 
 * */
public class KafkaSimpleProducerThread extends Thread {

	public KafkaSimpleProducerThread() {
	}

	public KafkaSimpleProducerThread(Runnable target) {
		super(target);
	}

	public KafkaSimpleProducerThread(String name) {
		super(name);
	}

	public KafkaSimpleProducerThread(ThreadGroup group, Runnable target) {
		super(group, target);
	}

	public KafkaSimpleProducerThread(ThreadGroup group, String name) {
		super(group, name);
	}

	public KafkaSimpleProducerThread(Runnable target, String name) {
		super(target, name);
	}

	public KafkaSimpleProducerThread(ThreadGroup group, Runnable target,
			String name) {
		super(group, target, name);
	}

	public KafkaSimpleProducerThread(ThreadGroup group, Runnable target,
			String name, long stackSize) {
		super(group, target, name, stackSize);
	}
	
	/**
	 * The props defined three mandatory attributes with values for producing Kafka messages.
	 * Kafka server usually runs on multiple machines. One of the server takes the leader role.
	 * I was using my local development machine to configure two Kafka server nodes, one on port 9093, 
	 * while the other was on 9094 port. Eache message in Kafka is basically key-value pair. You 
	 * need to tells API what formats you would like to use for the key and the value. I was using string 
	 * message as key and value format. We can start publishing message once the connection has established.
	 * In the following example, my program is going to publish message to topic name called "newtopic".
	 * This topic was created before I run this program by using Kafka command. In the example, I let the 
	 * test program automatically generating key in format of "mykey#".
	 * */
	@Override
	public void run(){
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9093,localhost:9094");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
		int i=0;
		try(KafkaSimpleProducer<String, String> producer = new KafkaSimpleProducer<String, String>()){
			producer.createProducer(props);
			while(true){
				String topic = "newtopic";
				String key = "mykey"+i;
				String value = "myvalue:" + System.currentTimeMillis() + "::=>dgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsfdgsdsf";
				System.out.println("Publish to topic " + topic + "[Key:" + key + ", Value:" + value + "]");
				producer.asyncPublish(topic, key, value);
				i++;
//				synchronized(this){
//					try {
//						this.wait(1000);
//					} catch (InterruptedException e) {
//						break;
//					}
//				}
			}
		}
	}

}
