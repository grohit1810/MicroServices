import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;

public class DataCleaningConsumer implements Runnable {
	//Active MQ administrative objects
	ActiveMQConnectionFactory connectionFactory;
	Connection connection;
	Session session;
	Destination dest;
	MessageConsumer consumer;
	//constructor to initialize ActiveMQ objects
	public DataCleaningConsumer() throws JMSException {
		connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
		connection = connectionFactory.createConnection();			
		session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		dest = session.createQueue("EnrichedDataQueue");
		consumer = session.createConsumer(dest);
	}
	
	//set listener object to this message consumer
	@Override
	public void run() {
		try {
			DataCleaningListener myListener = new DataCleaningListener();
			myListener.setConsumerObjectsToClose(session, connection);
			consumer.setMessageListener(myListener);
			connection.start();
		} 
		catch (Exception e) {
			e.printStackTrace();
		}
	}

}
