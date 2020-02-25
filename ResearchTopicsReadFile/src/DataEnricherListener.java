import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
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
public class DataEnricherListener implements MessageListener {
	// locationID lookup table
	public HashMap<String,String> locationIdMap = new HashMap<String,String>();
	//payment type lookup table
	public HashMap<String,String> paymentType = new HashMap<String,String>();
	ActiveMQConnectionFactory connectionFactory;
	Connection connection, consumerConnection;
	Session session, consumerSession;
	Destination dest;
	MessageProducer producer;
	
	//constructor to initialize ActiveMQ objects
	public DataEnricherListener() throws JMSException, IOException {
		connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
		connection = connectionFactory.createConnection();
		connection.start();
		session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		dest = session.createQueue("EnrichedDataQueue");
		producer = session.createProducer(dest);
		producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
		InitializeLocationPaymentMap();
	}
	//function to initialize the location and payment map
	public void InitializeLocationPaymentMap() throws IOException{
		paymentType.put("1", "Credit Card");
		paymentType.put("2", "Cash");
		paymentType.put("3", "No Charge");
		paymentType.put("4", "Dispute");
		paymentType.put("5", "Unknown");
		paymentType.put("6", "Voided Trip");
		BufferedReader csvReader = new BufferedReader(new FileReader("taxi+_zone_lookup.csv"));
		String row;
		boolean first=true;
		while ((row = csvReader.readLine()) != null) {
			if(first) {
				first = false;
			}
			else {
			String[] line = row.split(",");
			String boroughZone = "("+line[1].trim()+","+line[2].trim()+")";
			locationIdMap.put(line[0], boroughZone);
			}
		}
		csvReader.close();
	}
	//fucntion to encrich the data with correct values
	public List<String> PreProcessData(String data)  {
			
		String[] strippedData = data.split(",");
		List<String> trip = Arrays.asList(strippedData);
		trip.set(7, locationIdMap.get(trip.get(7)));
		trip.set(8, locationIdMap.get(trip.get(8)));
		trip.set(9, paymentType.get(trip.get(9)));
		return trip;
	}
	// function to format data back into string
	public String FormatData(List<String> trip) {
		String cleanedTrip = "";
		for(String data : trip) {
			cleanedTrip += data + "\t";
		}
		return cleanedTrip.trim();
	}
	public void setConsumerObjectsToClose(Session session, Connection connection) {
		consumerConnection = connection;
		consumerSession = session;
	}
	//function to close all ActiveMQ connections
	public void closeAllConnections() throws JMSException {
		producer.close();
		session.close();
		connection.close();
		consumerSession.close();
		consumerConnection.close();
	}
	// this function is called when the queue is dequeued, this function pushes the data for next micro service
	public void onMessage(Message message) {
		try {
			if (message instanceof TextMessage) {
				TextMessage textMessage = (TextMessage) message;
				String text = textMessage.getText();
				if(text.equals("EXITCONNECTION")) {
					TextMessage finalMsg = session.createTextMessage("EXITCONNECTION");
					producer.send(finalMsg);
					closeAllConnections();
					return;
				}
				List<String> tripDetails = PreProcessData(text);
				String cleanedTrip = FormatData(tripDetails);
				TextMessage message1 = session.createTextMessage(cleanedTrip);
    			producer.send(message1);
    		}	
			else {
				System.out.println("Received: " + message);
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		
	}

}
