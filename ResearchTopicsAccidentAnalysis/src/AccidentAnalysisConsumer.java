import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;

public class AccidentAnalysisConsumer implements Runnable {
	//ActiveMQ administrative objects
	ActiveMQConnectionFactory connectionFactory;
	Connection connection;
	Session session;
	Destination dest;
	MessageConsumer consumer;
	//constructor to initialize ActiveMQ objects
	public AccidentAnalysisConsumer() throws JMSException {
		connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
		connection = connectionFactory.createConnection();			
		session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);		
		dest = session.createQueue("AccidentAnalysisQueue");		
		consumer = session.createConsumer(dest);
	}
	//set listener object to this message consumer
	@Override
	public void run() {
		try {
			AccidentAnalysisListener myListener = new AccidentAnalysisListener();
			myListener.setConsumerObjectsToClose(session, connection);
			consumer.setMessageListener(myListener);
			connection.start();
			} 
		catch (Exception e) {
			e.printStackTrace();
		}
		
	}
}
