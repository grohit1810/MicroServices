
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
import javax.jms.Session;
import javax.jms.TextMessage;


public class PeakLocationAnalysisListener implements MessageListener{
	//Active MQ administrative objects
	Connection consumerConnection;
	Session consumerSession;
	public static BufferedWriter writer;
	//hashpmap to store state information to calculate analytics
	public static HashMap<String,HashMap<String,ArrayList<ArrayList<String>>>> peakLocationMap = 
			new HashMap<String,HashMap<String,ArrayList<ArrayList<String>>>>();
	//constructor to initialize a file writer for the analysis results
	public PeakLocationAnalysisListener() throws IOException {
		writer = new BufferedWriter(new FileWriter("PeakLocationAnalysisResults.txt"));
	}
	//function to store state information for peak location analysis in a day
	public void StoreLocationAnalysisDetails(ArrayList<String> tripData) throws IOException {
		String date = tripData.get(0);
		String location = tripData.get(2);
		if(PeakLocationAnalysisListener.peakLocationMap.size()>0) {
			Entry<String, HashMap<String, ArrayList<ArrayList<String>>>> entry = 
					PeakLocationAnalysisListener.peakLocationMap.entrySet().iterator().next();
			String dateInMap = entry.getKey();
			if(!dateInMap.equals(date)) { // new date. Print analytics and flush the date in the hashmap
				CalculateBusiestLocationDayWise(dateInMap);
				writer.write("*******************************************************************************\n");
				PeakLocationAnalysisListener.peakLocationMap.remove(dateInMap);
			}
		}
		
		if(PeakLocationAnalysisListener.peakLocationMap.containsKey(date))
		{
			HashMap<String,ArrayList<ArrayList<String>>> locationMap = 
					PeakLocationAnalysisListener.peakLocationMap.get(date);
			
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
			PeakLocationAnalysisListener.peakLocationMap.put(date, locationMap);
		}
	}
	//Function to calculate frequency of a location in a day
	public int CalculateFrequencyForLocationInDay(String date,String locationData) {
		int frequency = -1;
		if(PeakLocationAnalysisListener.peakLocationMap.containsKey(date) && PeakLocationAnalysisListener.peakLocationMap.get(date).containsKey(locationData)) {
			frequency = PeakLocationAnalysisListener.peakLocationMap.get(date).get(locationData).size();
			System.out.println("Frequency of "+ locationData + ": " + frequency + " at day :" + date );
		}
		else 
			System.out.println("No trips for this time and location");
		return frequency;
	}
	//Function to calculate and print frequency of all locations in a day
	public void CalculateFrequencyDayTumbling(String date) {
		if(PeakLocationAnalysisListener.peakLocationMap.containsKey(date)) {
			HashMap<String,ArrayList<ArrayList<String>>> locationMap = PeakLocationAnalysisListener.peakLocationMap.get(date);
			System.out.println("Frequency of all locations at this :" + date);
			locationMap.forEach((key,value) -> System.out.println(key + " -> " + value.size()));
		}
		else 
			System.out.println("No trips for this day");
	}
	//function to calculate top 3 busiest location in a day
	public void CalculateBusiestLocationDayWise(String date) throws IOException {
		HashMap<String,Integer> locFreq = new HashMap<String,Integer>();
		if(PeakLocationAnalysisListener.peakLocationMap.containsKey(date)) {
			for(Entry<String,ArrayList<ArrayList<String>>> entry : PeakLocationAnalysisListener.peakLocationMap.get(date).entrySet() ) {
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
				StoreLocationAnalysisDetails(cleanedTripList);
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
		if(PeakLocationAnalysisListener.peakLocationMap.size()>0) {
			Entry<String, HashMap<String, ArrayList<ArrayList<String>>>> entry = 
					PeakLocationAnalysisListener.peakLocationMap.entrySet().iterator().next();
			String dateInMap = entry.getKey();
			CalculateBusiestLocationDayWise(dateInMap);
			writer.write("*******************************************************************************\n");
			PeakLocationAnalysisListener.peakLocationMap.remove(dateInMap);
		}
		consumerSession.close();
		consumerConnection.close();
		writer.close();
	}

	public void setConsumerObjectsToClose(Session session, Connection connection) {
		consumerConnection = connection;
		consumerSession = session;
		
	}
	
}
