import javax.jms.JMSException;

//main function to start Tumbling window micro service
public class TumblingWindowCoordinator{
	public static void main(String[] args) throws JMSException {
		TumblingWindowConsumer consumer = new TumblingWindowConsumer();
		consumer.run();
	}
}
