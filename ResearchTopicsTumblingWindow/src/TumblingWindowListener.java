import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringJoiner;
import java.util.stream.Collectors;

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

public class TumblingWindowListener implements MessageListener {
	//hashmap to store state information of the taxi data(day tumbling and hour tumbling)
	public static HashMap<String,HashMap<String,HashMap<String,Integer>>> tumblingWindowMap = 
			new HashMap<>();
	public static BufferedWriter writer;
	//Active MQ administrative objects
	ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
	Connection connection, consumerConnection;
	Session session, consumerSession;
	Destination dest;
	MessageProducer producer;
	//constructor to initialize ActiveMQ objects
	public TumblingWindowListener() throws JMSException, IOException {
		connection = connectionFactory.createConnection();
		connection.start();
		session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		dest = session.createQueue("AccidentAnalysisQueue");
		producer = session.createProducer(dest);
		producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
		writer = new BufferedWriter(new FileWriter("TumblingWindowResults.txt"));
	}
	//function to run analytics on the trip data.
	public void RunAnalytics(String dateInMap) throws JMSException, IOException {
		ArrayList<String> hourMapKeys = new ArrayList<>(TumblingWindowListener.tumblingWindowMap.get(dateInMap).keySet());
		ArrayList<Integer> sortedHourMapKeys = new ArrayList<>();
		for(String key : hourMapKeys) {
			sortedHourMapKeys.add(Integer.parseInt(key));
		}
		Collections.sort(sortedHourMapKeys);
		String printHour = Integer.toString(sortedHourMapKeys.get(sortedHourMapKeys.size()-1));
		CalculateBusiestLocationHourWise(dateInMap,printHour,true);
		ArrayList<String> busiestHours = CalculateBusiestHourDayWise(dateInMap);
		for(String busyHour : busiestHours) {
			ArrayList<String> busiestBoroughs = CalculateBusiestLocationHourWise(dateInMap,busyHour,false);
			for(String busyBorough : busiestBoroughs) {
				//send busy location, date, busy hour to a queue for accident analysis
				String accidentRequirement = dateInMap + "\t" + busyHour + "\t" + busyBorough;
				TextMessage message = session.createTextMessage(accidentRequirement);
				producer.send(message);
			}
		}
		//writer.write("*******************************************************************************\n");
	}
	//function to store state information in the hashmap
	public void StoreFrequencyHourTumbling(ArrayList<String> tripData) throws JMSException, IOException {
		
		String date = tripData.get(0);
		String hour = tripData.get(1).split(":")[0];
		String location = tripData.get(2);
		if(TumblingWindowListener.tumblingWindowMap.size()>0) {
			Entry<String, HashMap<String, HashMap<String, Integer>>> entry = 
					TumblingWindowListener.tumblingWindowMap.entrySet().iterator().next();
			String dateInMap = entry.getKey();
			if(!dateInMap.equals(date)) { // new date. Print analytics and flush the date in the hashmap
				RunAnalytics(dateInMap);
				TumblingWindowListener.tumblingWindowMap.remove(dateInMap);
			}
		}
		
		if(TumblingWindowListener.tumblingWindowMap.containsKey(date))
		{
			HashMap<String,HashMap<String,Integer>> hourMap = 
					TumblingWindowListener.tumblingWindowMap.get(date);
			if(hourMap.containsKey(hour)) {
				HashMap<String,Integer> locationMap = 
						hourMap.get(hour);
				if(locationMap.containsKey(location)) {
					locationMap.put(location, locationMap.get(location)+1);
				}
				else {
					locationMap.put(location, 1);
				}
			}
			else {
				ArrayList<String> hourMapKeys = new ArrayList<>(hourMap.keySet());
				ArrayList<Integer> sortedHourMapKeys = new ArrayList<>();
				for(String key : hourMapKeys) {
					sortedHourMapKeys.add(Integer.parseInt(key));
				}
				Collections.sort(sortedHourMapKeys);
				String printHour = Integer.toString(sortedHourMapKeys.get(sortedHourMapKeys.size()-1));
				CalculateBusiestLocationHourWise(date,printHour,true);
				
				HashMap<String,Integer> locationMap = new HashMap<>();
				locationMap.put(location, 1);
				hourMap.put(hour, locationMap);
			}
		}
		else {
			HashMap<String,Integer> locationMap = new HashMap<>();
			locationMap.put(location, 1);
			HashMap<String,HashMap<String,Integer>> hourMap = new HashMap<>();
			
			hourMap.put(hour, locationMap);
			TumblingWindowListener.tumblingWindowMap.put(date, hourMap);
		}
	}
	// function to calculate frequency for a location at a date and hour
	public int CalculateFrequencyHourTumblingForLocation(String date, String hour, String locationData) {
		int frequency = -1;
		if(TumblingWindowListener.tumblingWindowMap.containsKey(date) && TumblingWindowListener.tumblingWindowMap.get(date).containsKey(hour) &&
				TumblingWindowListener.tumblingWindowMap.get(date).get(hour).containsKey(locationData)) {
			System.out.println("Frequency of "+ locationData +" on day: " +date + " in hour: " + hour);
			frequency = TumblingWindowListener.tumblingWindowMap.get(date).get(hour).get(locationData);
		}
		else 
			System.out.println("No trips for this time and location");
		return frequency;
	}
	//frequency of all locations at a date and hour
	public void CalculateFrequencyHourTumbling(String date, String hour) {
		if(TumblingWindowListener.tumblingWindowMap.containsKey(date) && TumblingWindowListener.tumblingWindowMap.get(date).containsKey(hour)) {
			HashMap<String,Integer> locationMap = TumblingWindowListener.tumblingWindowMap.get(date).get(hour);
			System.out.println("Frequency of all locations on day: " + date + " in hour: " + hour);
			locationMap.forEach((key,value) -> System.out.println(key + " -> " + value));
		}
		else 
			System.out.println("No trips for this time");
	}
	//function to calculate top 3 peak hours of the day
	public ArrayList<String> CalculateBusiestHourDayWise(String date) throws IOException {
		HashMap<String,Integer> hourFreq = new HashMap<String,Integer>();
		ArrayList<String> busiestHours = new ArrayList<>();
		if(TumblingWindowListener.tumblingWindowMap.containsKey(date)) {
			for (Entry<String, HashMap<String, Integer>> entry : TumblingWindowListener.tumblingWindowMap.get(date).entrySet()) {
			    int freqCount = 0;
			    for(Entry<String, Integer> currentLoc : entry.getValue().entrySet()) {
			    	freqCount += currentLoc.getValue();
			    }
			    hourFreq.put(entry.getKey(), freqCount);
			}
		
			Map<String, Integer> sortedMap = hourFreq.entrySet()
					.stream().sorted(Entry.comparingByValue(Comparator.reverseOrder()))
					.collect(Collectors.toMap(Entry::getKey, Entry::getValue,(e1, e2) -> e1, LinkedHashMap::new));
			int counter = 0;
			
			writer.write("Top 3 peak hour in the day : " + date + "{");
			System.out.println();
			System.out.print("Top 3 peak hour in the day : " + date + "{");
			StringJoiner joiner = new StringJoiner(", ");
			for(Entry<String,Integer> locEntry : sortedMap.entrySet()) {
				if(counter<3) {
					
					busiestHours.add(locEntry.getKey());
					joiner.add(locEntry.getKey() + ": " + locEntry.getValue());
				}
				else
					break;
				counter++;
			}
			writer.write(joiner.toString()+"}\n");
			System.out.println(joiner.toString()+"}");
			System.out.println();
		}
		return busiestHours;
	}
	//function to calculate the busiest location for any given specific hour
	public ArrayList<String> CalculateBusiestLocationHourWise(String date, String hour, boolean printFlag) throws IOException {
		HashMap<String,Integer> locFreq = new HashMap<>();
		ArrayList<String> busiestBoroughs = new ArrayList<>();
		HashMap<String,Integer> boroughFreq = new HashMap<>();
		if(TumblingWindowListener.tumblingWindowMap.containsKey(date) && TumblingWindowListener.tumblingWindowMap.get(date).containsKey(hour)) {
			for(Entry<String, Integer> locationEntry : TumblingWindowListener.tumblingWindowMap.get(date).get(hour).entrySet() ) {
				locFreq.put(locationEntry.getKey(), locationEntry.getValue());
				String borough = locationEntry.getKey().split(",")[0].substring(1);
				borough = borough.substring(1,borough.length()-1).toLowerCase();
				if(boroughFreq.containsKey(borough)) {
					boroughFreq.put(borough, boroughFreq.get(borough)+locationEntry.getValue());
				}
				else {
					boroughFreq.put(borough, locationEntry.getValue());
				}
			}
		
			Map<String, Integer> sortedMap = locFreq.entrySet()
					.stream().sorted(Entry.comparingByValue(Comparator.reverseOrder()))
					.collect(Collectors.toMap(Entry::getKey, Entry::getValue,(e1, e2) -> e1, LinkedHashMap::new));
			int counter = 0;
			
			writer.write("Top 3 busiest location in the day : " + date + " hour : " + hour + "{");
			if(printFlag)
				System.out.print("Top 3 busiest location in the day : " + date + " hour : " + hour + "{");
			StringJoiner joiner = new StringJoiner(", ");
			//sortedMap.forEach((key,value) -> System.out.println(key + " -> " + value));
			for(Entry<String,Integer> locEntry : sortedMap.entrySet()) {
				if(counter<3) {
					joiner.add(locEntry.getKey() + ": " + locEntry.getValue());
				}
				else
					break;
				counter++;
			}
			writer.write(joiner.toString()+"}\n");
			if(printFlag)
				System.out.println(joiner.toString()+"}");
			Map<String, Integer> sortedBoroughMap = boroughFreq.entrySet()
					.stream().sorted(Entry.comparingByValue(Comparator.reverseOrder()))
					.collect(Collectors.toMap(Entry::getKey, Entry::getValue,(e1, e2) -> e1, LinkedHashMap::new));
			counter = 0;
			for(Entry<String,Integer> boroughEntry : sortedBoroughMap.entrySet()) {
				if(counter>3)
					break;
				if(!boroughEntry.getKey().equals("unknown")){
					busiestBoroughs.add(boroughEntry.getKey());
					counter++;
				}
			}
		}
		return busiestBoroughs;
	}
	//set listener object to this message consumer
	@Override
	public void onMessage(Message msg) {
		try {
			if (msg instanceof TextMessage)
			{
				TextMessage textMessage = (TextMessage) msg;
				
				String text = textMessage.getText();
				if(text.equals("EXITCONNECTION")) {
					TextMessage finalMsg = session.createTextMessage("EXITCONNECTION");
					producer.send(finalMsg);
					closeAllConnections();
					return;
				}
				String[] cleanedTrip = text.split("\t");
				ArrayList<String> cleanedTripList = new ArrayList<>();
				for(int i=0;i<cleanedTrip.length;i++)
					cleanedTripList.add(cleanedTrip[i]);
				StoreFrequencyHourTumbling(cleanedTripList);
				}
			 else {
				System.out.println("Received: " + msg);
			}
		}
		catch (Exception e) {
			e.printStackTrace();;
		}
	}

	private void closeAllConnections() throws JMSException, IOException {
		if(TumblingWindowListener.tumblingWindowMap.size()>0) {
			Entry<String, HashMap<String,HashMap<String, Integer>>> entry = 
					TumblingWindowListener.tumblingWindowMap.entrySet().iterator().next();
			String dateInMap = entry.getKey();
			RunAnalytics(dateInMap);
			TumblingWindowListener.tumblingWindowMap.remove(dateInMap);
		}
		producer.close();
		session.close();
		connection.close();
		consumerSession.close();
		consumerConnection.close();
		writer.close();
	}

	public void setConsumerObjectsToClose(Session session,Connection connection) {
		consumerConnection = connection;
		consumerSession = session;
		
	}

}
