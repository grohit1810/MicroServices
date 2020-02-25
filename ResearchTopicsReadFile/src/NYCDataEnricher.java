import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import org.apache.activemq.ActiveMQConnectionFactory;

public class NYCDataEnricher implements Runnable {
	//Active MQ administrative objects
	ActiveMQConnectionFactory connectionFactory ;
	Connection connection;
	Session session ;
	Destination dest ;
	MessageConsumer consumer;
	//constructor to initialize ActiveMQ objects
	public NYCDataEnricher() throws JMSException {
		connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
		connection = connectionFactory.createConnection();			
		session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		dest = session.createQueue("StraightFromFileDataQueue");
		consumer = session.createConsumer(dest);
	}
	public void run() {
		try {
			DataEnricherListener myListener = new DataEnricherListener();
			myListener.setConsumerObjectsToClose(session, connection);
			//set listener object for this microservice
			consumer.setMessageListener(myListener);
			connection.start();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
}
