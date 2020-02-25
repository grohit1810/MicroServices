import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;


public class TumblingWindowConsumer implements Runnable {
	//Active MQ administrative objects
	ActiveMQConnectionFactory connectionFactory;
	Connection connection;
	Session session;
	Destination dest;
	MessageConsumer consumer;
	//constructor to initialize ActiveMQ objects
	public TumblingWindowConsumer() throws JMSException {
		connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
		connection = connectionFactory.createConnection();			
		session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		dest = session.createQueue("ProcessedDataQueue1");
		consumer = session.createConsumer(dest);
	}
	//set listener object to this message consumer
	@Override
	public void run() {
		try {
			TumblingWindowListener myListener = new TumblingWindowListener();
			myListener.setConsumerObjectsToClose(session, connection);
			connection.start();
			consumer.setMessageListener(myListener);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}