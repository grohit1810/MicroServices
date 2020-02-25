import javax.jms.JMSException;

public class PeakLocationAnalysisCoordinator {
	//main function to call Peak location analysis consumer
	public static void main(String[] args) throws JMSException {
		PeakLocationAnalysisConsumer consumer = new PeakLocationAnalysisConsumer();
		consumer.run();
	}
}
