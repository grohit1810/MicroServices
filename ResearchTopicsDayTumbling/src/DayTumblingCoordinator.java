import javax.jms.JMSException;

public class DayTumblingCoordinator {
	public static void main(String[] args) throws JMSException {
		DayTumblingConsumer consumer = new DayTumblingConsumer();
		consumer.run();
	}
}
