import javax.jms.JMSException;

//main function to start Data cleaning micro service
public class DataCleaningCoordinator {
	public static void main(String[] args) throws JMSException {
		DataCleaningConsumer consumer = new DataCleaningConsumer();
		consumer.run();
	}
}
