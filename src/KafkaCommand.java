import kafka.admin.TopicCommand;

/**
 * @author Chih Wen Tai
 * 
 * This class is a demo on how to run Kafka command programmatically.
 * There is no parameters needed to run this program. 
 * However, you do need a Kafka and a zookeeper processes running before you run this class
 * */
public class KafkaCommand {

	public static void main(String[] args) {
		String[] options = new String[]{  
			    "--list",  
			    "--zookeeper",  
			    "localhost:2181"  
		};  
		TopicCommand.main(options);
	}

}
