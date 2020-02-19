import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;


public class HourTumblingConsumer implements Runnable {

	ActiveMQConnectionFactory connectionFactory ;
	Connection connection;
	Session session ;
	Destination dest ;
	MessageConsumer consumer;
	public HourTumblingConsumer() throws JMSException {
		connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
		
		connection = connectionFactory.createConnection();			
		
		session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		
		dest = session.createQueue("ProcessedDataQueue1");
		
		consumer = session.createConsumer(dest);
	}
	@Override
	public void run() {

		try {
			
			HourTumblingListener myListener = new HourTumblingListener();
			myListener.setConnectionObjectToClose(connection);
			
			consumer.setMessageListener(myListener);
			
			connection.start();
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

}
