
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
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


public class MyListener implements MessageListener {
	// Administrative object
	public HashMap<String,String> locationIdMap = new HashMap<String,String>();
	public HashMap<String,String> cashType = new HashMap<String,String>();
	ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
	Connection connection1;
	Session session1;
	Destination dest1;
	MessageProducer producer1;
	Connection connection2;
	Session session2;
	Destination dest2;
	MessageProducer producer2;
	Connection consumerConnection;
	
	public MyListener() throws JMSException, IOException {
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
		
		CreateLocationMap();
	}
	public void CreateLocationMap() throws IOException{
		cashType.put("1", "Credit Card");
		cashType.put("2", "Cash");
		cashType.put("3", "No Charge");
		cashType.put("4", "Dispute");
		cashType.put("5", "Unknown");
		cashType.put("6", "Voided Trip");
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
	public List<String> PreProcessData(String data)  {
			
		String[] strippedData = data.split(",");
		List<String> trip = Arrays.asList(strippedData);
//		System.out.println(trip);
		trip.set(7, locationIdMap.get(trip.get(7)));
		trip.set(8, locationIdMap.get(trip.get(8)));
		trip.set(9, cashType.get(trip.get(9)));
//		System.out.println(trip);
		return trip;
	}
	public String CleanTripData(List<String> trip) {
		//ArrayList<String> tripDetails = new ArrayList<>();
//		System.out.println(trip.get(0));
		String pickupDate = trip.get(1).split(" ")[0];
		String pickupTime = trip.get(1).split(" ")[1];
		if(pickupTime.startsWith("0"))
			pickupTime = pickupTime.substring(1);
		String pickupLocation = trip.get(7);
//		System.out.println("Rohit Debug , Date: "+pickupDate+" Time: "+pickupTime+ " Location: "+pickupLocation);
		
//		tripDetails.add(pickupTime);
//		tripDetails.add(pickupLocation);
		//return tripDetails;
		String cleanedTrip = pickupDate + "\t" + pickupTime + "\t" +pickupLocation;
		return cleanedTrip;
	}
	public void setConnectionObjectToClose(Connection connection) {
		consumerConnection = connection;
	}
	public void closeAllConnections() throws JMSException {
		//connection.stop();
		//connection.close();
		//consumerConnection.stop();
		//consumerConnection.close();
	}
	public void onMessage(Message message) {
		
		try {
			
			if (message instanceof TextMessage)
			{
				TextMessage textMessage = (TextMessage) message;
				
				String text = textMessage.getText();
				if(text.equals("EXITCONNECTION")) {
					TextMessage finalMsg = session1.createTextMessage("EXITCONNECTION");
					producer1.send(finalMsg);
					producer2.send(finalMsg);
					closeAllConnections();
					return;
				}
								
				//System.out.println("Received: " + text);
				List<String> tripDetails = PreProcessData(text);
				String cleanedTrip = CleanTripData(tripDetails);
				
				
    			TextMessage message1 = session1.createTextMessage(cleanedTrip);
    			TextMessage message2 = session2.createTextMessage(cleanedTrip);
    			
    			//System.out.println("sent message: " + message1.hashCode() + " : " + Thread.currentThread().getName());
    			producer1.send(message1);
    			producer2.send(message2);
				}
				
			 else {
				System.out.println("Received: " + message);
			}
			
		}
		catch (JMSException e)
		{
			e.getStackTrace();
		}
		
	}

}
