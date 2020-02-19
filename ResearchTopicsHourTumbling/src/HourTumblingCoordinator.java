import javax.jms.JMSException;

public class HourTumblingCoordinator{
	public static void main(String[] args) throws JMSException {
		HourTumblingConsumer consumer = new HourTumblingConsumer();
		consumer.run();
	}
}
