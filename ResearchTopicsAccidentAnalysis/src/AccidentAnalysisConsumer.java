import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;

public class AccidentAnalysisConsumer implements Runnable {
	ActiveMQConnectionFactory connectionFactory;
	Connection connection;
	Session session;
	Destination dest;
	MessageConsumer consumer;
	public AccidentAnalysisConsumer() throws JMSException {
		connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
		connection = connectionFactory.createConnection();			
		session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);		
		dest = session.createQueue("AccidentAnalysisQueue");		
		consumer = session.createConsumer(dest);
	}
	@Override
	public void run() {
		try {
			AccidentAnalysisListener myListener = new AccidentAnalysisListener();
			myListener.setConnectionObjectToClose(connection);
			consumer.setMessageListener(myListener);
			connection.start();
			} 
		catch (Exception e) {
			e.printStackTrace();
		}
		
	}
}
