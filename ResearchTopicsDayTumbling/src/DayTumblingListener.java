
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;


public class DayTumblingListener implements MessageListener{
	Connection consumerConnectionToClose;
	public static BufferedWriter writer;
	public static HashMap<String,HashMap<String,ArrayList<ArrayList<String>>>> mapDayTumbling = 
			new HashMap<String,HashMap<String,ArrayList<ArrayList<String>>>>();
	public DayTumblingListener() throws IOException {
		writer = new BufferedWriter(new FileWriter("DayTumblingResults.txt"));
	}
	public void StoreFrequencyDayTumbling(ArrayList<String> tripData) throws IOException {
		String date = tripData.get(0);
		String location = tripData.get(2);
		if(DayTumblingListener.mapDayTumbling.size()>0) {
			Entry<String, HashMap<String, ArrayList<ArrayList<String>>>> entry = 
					DayTumblingListener.mapDayTumbling.entrySet().iterator().next();
			String dateInMap = entry.getKey();
			if(!dateInMap.equals(date)) { // new date. Print analytics and flush the date in the hashmap
				CalculateBusiestLocationDayWise(dateInMap);
				writer.write("*******************************************************************************\n");
				DayTumblingListener.mapDayTumbling.remove(dateInMap);
			}
		}
		
		if(DayTumblingListener.mapDayTumbling.containsKey(date))
		{
			HashMap<String,ArrayList<ArrayList<String>>> locationMap = 
					DayTumblingListener.mapDayTumbling.get(date);
			
			if(locationMap.containsKey(location)) {
				ArrayList<ArrayList<String>> locationList = locationMap.get(location);
				locationList.add(tripData);
			}
			else {
				ArrayList<ArrayList<String>> locationList = new ArrayList<>();
				locationList.add(tripData);
				locationMap.put(location, locationList);
			}
		}
		else {
			HashMap<String,ArrayList<ArrayList<String>>> locationMap = new HashMap<String,ArrayList<ArrayList<String>>>();
			ArrayList<ArrayList<String>> locationList=new ArrayList<>();
			locationList.add(tripData);
			locationMap.put(location, locationList);
			DayTumblingListener.mapDayTumbling.put(date, locationMap);
		}
	}
	public int CalculateFrequencyDayTumblingForLocation(String date,String locationData) {
		int frequency = -1;
		if(DayTumblingListener.mapDayTumbling.containsKey(date) && DayTumblingListener.mapDayTumbling.get(date).containsKey(locationData)) {
			frequency = DayTumblingListener.mapDayTumbling.get(date).get(locationData).size();
			System.out.println("Frequency of "+ locationData + ": " + frequency + " at day :" + date );
		}
		else 
			System.out.println("No trips for this time and location");
		return frequency;
	}
	public void CalculateFrequencyDayTumbling(String date) {
		if(DayTumblingListener.mapDayTumbling.containsKey(date)) {
			HashMap<String,ArrayList<ArrayList<String>>> locationMap = DayTumblingListener.mapDayTumbling.get(date);
			System.out.println("Frequency of all locations at this :" + date);
			locationMap.forEach((key,value) -> System.out.println(key + " -> " + value.size()));
		}
		else 
			System.out.println("No trips for this day");
	}
	public void CalculateBusiestLocationDayWise(String date) throws IOException {
		HashMap<String,Integer> locFreq = new HashMap<String,Integer>();
		if(DayTumblingListener.mapDayTumbling.containsKey(date)) {
			for(Entry<String,ArrayList<ArrayList<String>>> entry : DayTumblingListener.mapDayTumbling.get(date).entrySet() ) {
				locFreq.put(entry.getKey(), entry.getValue().size());
			}
		}
		Map<String, Integer> sortedMap = locFreq.entrySet()
				.stream().sorted(Entry.comparingByValue(Comparator.reverseOrder()))
				.collect(Collectors.toMap(Entry::getKey, Entry::getValue,(e1, e2) -> e1, LinkedHashMap::new));
		//sortedMap.forEach((key,value) -> System.out.println(key + " -> " + value));
		int counter = 0;
		writer.write("Top 3 busiest location on the day: " + date + "\n");
		for(Entry<String,Integer> locEntry : sortedMap.entrySet()) {
			if(counter<3)
				writer.write(locEntry.getKey() + " -> " + locEntry.getValue() + "\n");
			else
				break;
			counter++;
		}
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
					closeAllConnections();
					return;
				}			
				//System.out.println("Received: " + text);
				String[] cleanedTrip = text.split("\t");
				//System.out.println(cleanedTrip[0]);
				ArrayList<String> cleanedTripList = new ArrayList<>();
				for(int i=0;i<cleanedTrip.length;i++)
					cleanedTripList.add(cleanedTrip[i]);
				StoreFrequencyDayTumbling(cleanedTripList);
				}
			 else {
				System.out.println("Received: " + msg);
			}
			
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		
	}
	private void closeAllConnections() throws JMSException, IOException {
		//CalculateFrequencyHourTumbling("2018-01-01 00:08:26");
		//CalculateBusiestHourDayWise("2018-01-01 00:08:26");
		//CalculateBusiestLocationHourWise("2018-01-01 00:08:26");
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
