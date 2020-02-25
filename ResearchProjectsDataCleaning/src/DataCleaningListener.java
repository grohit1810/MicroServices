import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;

public class DataCleaningListener implements MessageListener {
	//constructor to initialize ActiveMQ objects
	ActiveMQConnectionFactory connectionFactory;
	Connection connection1, connection2, consumerConnection;
	Session session1, session2, consumerSession;
	Destination dest1, dest2;
	MessageProducer producer1, producer2;
	//constructor to initialize ActiveMQ objects
	public DataCleaningListener() throws JMSException {
		connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
		connection1 = connectionFactory.createConnection();
		connection1.start();
		session1 = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);
		dest1 = session1.createQueue("ProcessedDataQueue1");
		producer1 = session1.createProducer(dest1);
		producer1.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
		
		connection2 = connectionFactory.createConnection();
		connection2.start();
		session2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
		dest2 = session2.createQueue("ProcessedDataQueue2");
		producer2 = session2.createProducer(dest2);
		producer2.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
	}
	//this function removes redundant data from the trip data.
	public String CleanTripData(String trip) {
		String[] tripSplit = trip.trim().split("\t");
		String pickupDate = tripSplit[1].trim().split(" ")[0];
		String pickupTime = tripSplit[1].trim().split(" ")[1];
		String pickupHour = Integer.toString(Integer.parseInt(pickupTime.split(":")[0]));
		pickupTime = pickupHour + ":" + pickupTime.split(":")[1];
		String pickupLocation = tripSplit[7];
		String cleanedTrip = pickupDate + "\t" + pickupTime + "\t" +pickupLocation;
		return cleanedTrip;
	}
	
	//this function is called when the DataCleaningQueue is dequeued, this functions cleans the data and pushes
	//it to two queues for the proceeding micro services.
	@Override
	public void onMessage(Message message) {
		try {
			if (message instanceof TextMessage) {
				TextMessage textMessage = (TextMessage) message;
				String text = textMessage.getText();
				if(text.equals("EXITCONNECTION")) {
					TextMessage finalMsg = session1.createTextMessage("EXITCONNECTION");
					producer1.send(finalMsg);
					producer2.send(finalMsg);
					closeAllConnections();
					return;
				}
				String cleanedTrip = CleanTripData(text);
				TextMessage message1 = session1.createTextMessage(cleanedTrip);
    			TextMessage message2 = session2.createTextMessage(cleanedTrip);
    			producer1.send(message1);
    			producer2.send(message2);
			}
			else {
				System.out.println("Received: " + message);
			}
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	//close all connections
	private void closeAllConnections() throws JMSException {
		producer1.close();
		session1.close();
		connection1.close();
		producer2.close();
		session2.close();
		connection2.close();
		consumerSession.close();
		consumerConnection.close();
	}
	public void setConsumerObjectsToClose(Session session, Connection connection) {
		consumerConnection = connection;
		consumerSession = session;
		
	}
}
