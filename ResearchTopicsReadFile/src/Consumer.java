
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;


public class Consumer implements Runnable {

	ActiveMQConnectionFactory connectionFactory ;
	Connection connection;
	Session session ;
	Destination dest ;
	MessageConsumer consumer;
	public Consumer() throws JMSException {
		connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
		
		connection = connectionFactory.createConnection();			
		
		session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		
		dest = session.createQueue("StraightFromFileDataQueue");
		
		consumer = session.createConsumer(dest);
	}
	public void run() {
		
		
		try {
			
			
			
			MyListener myListener = new MyListener();
			myListener.setConnectionObjectToClose(connection);
			
			consumer.setMessageListener(myListener);
			
			connection.start();
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	public void CloseConnectionActiveMQ() throws JMSException {
		connection.close();
		connection.stop();
	}

}
