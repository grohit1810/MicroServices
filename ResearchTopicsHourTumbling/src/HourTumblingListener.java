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

public class HourTumblingListener implements MessageListener {
	public static HashMap<String,HashMap<String,HashMap<String,Integer>>> mapHourTumbling = 
			new HashMap<>();
	public static BufferedWriter writer;
	Connection consumerConnectionToClose;
	ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
	Connection connection;
	Session session;
	Destination dest;
	MessageProducer producer;
	public HourTumblingListener() throws JMSException, IOException {
		connection = connectionFactory.createConnection();
		connection.start();
		session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		dest = session.createQueue("AccidentAnalysisQueue");
		producer = session.createProducer(dest);
		producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
		writer = new BufferedWriter(new FileWriter("HourTumblingResults.txt"));
	}
	public void RunAnalytics(String dateInMap) throws JMSException, IOException {
		
		ArrayList<String> hourMapKeys = new ArrayList<>(HourTumblingListener.mapHourTumbling.get(dateInMap).keySet());
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
				//System.out.println("Rohit debug msg: "+accidentRequirement);
				TextMessage message = session.createTextMessage(accidentRequirement);
				producer.send(message);
			}
		}
		writer.write("*******************************************************************************\n");
	}
	public void StoreFrequencyHourTumbling(ArrayList<String> tripData) throws JMSException, IOException {
		
		String date = tripData.get(0);
		String hour = tripData.get(1).split(":")[0];
		String location = tripData.get(2);
//		System.out.println("Rohit debug : ");
//		System.out.println("Date: " + date + " Hour: " + hour + " Location: " + location);
		if(HourTumblingListener.mapHourTumbling.size()>0) {
			Entry<String, HashMap<String, HashMap<String, Integer>>> entry = 
					HourTumblingListener.mapHourTumbling.entrySet().iterator().next();
			String dateInMap = entry.getKey();
			//System.out.println("Rohit print hash key: " +dateInMap+"New Value in check: " + date);
			if(!dateInMap.equals(date)) { // new date. Print analytics and flush the date in the hashmap
				RunAnalytics(dateInMap);
				HourTumblingListener.mapHourTumbling.remove(dateInMap);
			}
		}
		
		if(HourTumblingListener.mapHourTumbling.containsKey(date))
		{
			HashMap<String,HashMap<String,Integer>> hourMap = 
					HourTumblingListener.mapHourTumbling.get(date);
			if(hourMap.containsKey(hour)) {
				HashMap<String,Integer> locationMap = 
						hourMap.get(hour);
				if(locationMap.containsKey(location)) {
//					ArrayList<ArrayList<String>> locationList = locationMap.get(location);
//					locationList.add(tripData);
					locationMap.put(location, locationMap.get(location)+1);
				}
				else {
//					ArrayList<ArrayList<String>> locationList = new ArrayList<>();
//					locationList.add(tripData);
					locationMap.put(location, 1);
				}
			}
			else {
//				ArrayList<String> sortedHourMap = new ArrayList<>(hourMap.keySet());
//				Collections.sort(sortedHourMap);
//				String printHour = sortedHourMap.get(sortedHourMap.size()-1);
//				CalculateBusiestLocationHourWise(date,printHour,true);
				
				ArrayList<String> hourMapKeys = new ArrayList<>(hourMap.keySet());
				ArrayList<Integer> sortedHourMapKeys = new ArrayList<>();
				for(String key : hourMapKeys) {
					sortedHourMapKeys.add(Integer.parseInt(key));
				}
				Collections.sort(sortedHourMapKeys);
				String printHour = Integer.toString(sortedHourMapKeys.get(sortedHourMapKeys.size()-1));
				CalculateBusiestLocationHourWise(date,printHour,true);
				
				HashMap<String,Integer> locationMap = new HashMap<>();
//				ArrayList<ArrayList<String>> locationList=new ArrayList<>();
//				locationList.add(tripData);
				locationMap.put(location, 1);
				hourMap.put(hour, locationMap);
			}
		}
		else {
			HashMap<String,Integer> locationMap = new HashMap<>();
//			ArrayList<ArrayList<String>> locationList=new ArrayList<>();
//			locationList.add(tripData);
			locationMap.put(location, 1);
			HashMap<String,HashMap<String,Integer>> hourMap = new HashMap<>();
			
			hourMap.put(hour, locationMap);
			HourTumblingListener.mapHourTumbling.put(date, hourMap);
		}
	}
	public int CalculateFrequencyHourTumblingForLocation(String date, String hour, String locationData) {
		int frequency = -1;
		if(HourTumblingListener.mapHourTumbling.containsKey(date) && HourTumblingListener.mapHourTumbling.get(date).containsKey(hour) &&
				HourTumblingListener.mapHourTumbling.get(date).get(hour).containsKey(locationData)) {
			System.out.println("Frequency of "+ locationData +" on day: " +date + " in hour: " + hour);
			frequency = HourTumblingListener.mapHourTumbling.get(date).get(hour).get(locationData);
		}
		else 
			System.out.println("No trips for this time and location");
		return frequency;
	}
	public void CalculateFrequencyHourTumbling(String date, String hour) {
		if(HourTumblingListener.mapHourTumbling.containsKey(date) && HourTumblingListener.mapHourTumbling.get(date).containsKey(hour)) {
			HashMap<String,Integer> locationMap = HourTumblingListener.mapHourTumbling.get(date).get(hour);
			System.out.println("Frequency of all locations on day: " + date + " in hour: " + hour);
			locationMap.forEach((key,value) -> System.out.println(key + " -> " + value));
		}
		else 
			System.out.println("No trips for this time");
	}
	public ArrayList<String> CalculateBusiestHourDayWise(String date) throws IOException {
		HashMap<String,Integer> hourFreq = new HashMap<String,Integer>();
		ArrayList<String> busiestHours = new ArrayList<>();
		if(HourTumblingListener.mapHourTumbling.containsKey(date)) {
			for (Entry<String, HashMap<String, Integer>> entry : HourTumblingListener.mapHourTumbling.get(date).entrySet()) {
			    //System.out.println(entry.getKey() + " = " + entry.getValue());
			    int freqCount = 0;
			    for(Entry<String, Integer> currentLoc : entry.getValue().entrySet()) {
			    	freqCount += currentLoc.getValue();
			    }
			    //Integer freq = new Integer(freqCount);
			    hourFreq.put(entry.getKey(), freqCount);
			}
		
			Map<String, Integer> sortedMap = hourFreq.entrySet()
					.stream().sorted(Entry.comparingByValue(Comparator.reverseOrder()))
					.collect(Collectors.toMap(Entry::getKey, Entry::getValue,(e1, e2) -> e1, LinkedHashMap::new));
			//sortedMap.forEach((key,value) -> System.out.println(key + " -> " + value));
			int counter = 0;
			
			writer.write("Top 3 busiest hour in the day : " + date + "\n");
			System.out.println("Top 3 busiest hour in the day : " + date);
	//		sortedMap.forEach((key, value) -> System.out.println(key + ":" + value));
			for(Entry<String,Integer> locEntry : sortedMap.entrySet()) {
				if(counter<3) {
					writer.write(locEntry.getKey() + " -> " + locEntry.getValue() + "\n");
					System.out.println(locEntry.getKey() + " -> " + locEntry.getValue());
					busiestHours.add(locEntry.getKey());
				}
				else
					break;
				counter++;
			}
		}
		return busiestHours;
	}
	public ArrayList<String> CalculateBusiestLocationHourWise(String date, String hour, boolean printFlag) throws IOException {
		HashMap<String,Integer> locFreq = new HashMap<>();
		ArrayList<String> busiestBoroughs = new ArrayList<>();
		HashMap<String,Integer> boroughFreq = new HashMap<>();
		if(HourTumblingListener.mapHourTumbling.containsKey(date) && HourTumblingListener.mapHourTumbling.get(date).containsKey(hour)) {
			for(Entry<String, Integer> locationEntry : HourTumblingListener.mapHourTumbling.get(date).get(hour).entrySet() ) {
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
	//		System.out.println("Test : ");
	//		sortedMap.forEach((key,value) -> System.out.println(key + " -> " + value));
			int counter = 0;
			
			writer.write("Top 3 busiest location in the day : " + date + " hour : " + hour + "\n");
			if(printFlag)
				System.out.println("Top 3 busiest location in the day : " + date + " hour : " + hour);
			//sortedMap.forEach((key,value) -> System.out.println(key + " -> " + value));
			for(Entry<String,Integer> locEntry : sortedMap.entrySet()) {
				if(counter<3) {
					writer.write(locEntry.getKey() + " -> " + locEntry.getValue() + "\n");
					if(printFlag)
						System.out.println(locEntry.getKey() + " -> " + locEntry.getValue());
					//busiestLocations.add(locEntry.getKey());
				}
				else
					break;
				counter++;
			}
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
	@Override
	public void onMessage(Message msg) {
		// TODO Auto-generated method stub
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
								
				//System.out.println("Received: " + text);
				String[] cleanedTrip = text.split("\t");
				//System.out.println(cleanedTrip[0]);
				ArrayList<String> cleanedTripList = new ArrayList<>();
//				System.out.println("Rohit debug1: " + text);
				for(int i=0;i<cleanedTrip.length;i++)
					cleanedTripList.add(cleanedTrip[i]);
				StoreFrequencyHourTumbling(cleanedTripList);
				}
				
			 else {
				System.out.println("Received: " + msg);
			}
			
		}
		catch (Exception e)
		{
			e.printStackTrace();;
		}
		
		
	}

	private void closeAllConnections() throws JMSException, IOException {
		// TODO Auto-generated method stub
		//consumerConnectionToClose.stop();
		//consumerConnectionToClose.close();
		writer.close();
	}

	public void setConnectionObjectToClose(Connection connection) {
		// TODO Auto-generated method stub
		this.consumerConnectionToClose = connection;
		
	}

}
