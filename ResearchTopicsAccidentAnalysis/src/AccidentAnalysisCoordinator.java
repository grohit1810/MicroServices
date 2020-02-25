import javax.jms.JMSException;

public class AccidentAnalysisCoordinator {
	//main to start Accident Analysis micro service
	public static void main(String[] args) throws JMSException {
		AccidentAnalysisConsumer consumer = new AccidentAnalysisConsumer();
		consumer.run();
	}
}
