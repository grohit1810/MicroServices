import javax.jms.JMSException;

public class AccidentAnalysisCoordinator {

	public static void main(String[] args) throws JMSException {
		AccidentAnalysisConsumer consumer = new AccidentAnalysisConsumer();
		consumer.run();
		
	}

}
