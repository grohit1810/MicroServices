import java.io.*;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;

public class TestFile {
	public static HashMap<String,String> locationIdMap = new HashMap<>();
	public static HashMap<String,String> cashType = new HashMap<>();
	public static HashMap<String,HashMap<String,HashMap<String,ArrayList<ArrayList<String>>>>> mapHourTumbling = 
			new HashMap<>();
	public static HashMap<String,HashMap<String,ArrayList<ArrayList<String>>>> mapDayTumbling = 
			new HashMap<>();
	public static HashMap<String,HashMap<String,HashMap<String,Integer>>> mapAccidentInfo =
			new HashMap<>();
	public List<String> PreProcessData(String[] data)  {
		
		List<String> trip = Arrays.asList(data);
//		System.out.println(trip);
		trip.set(7, TestFile.locationIdMap.get(trip.get(7)));
		trip.set(8, TestFile.locationIdMap.get(trip.get(8)));
		trip.set(9, TestFile.cashType.get(trip.get(9)));
//		System.out.println(trip);
		return trip;
	}
	public ArrayList<String> CleanData(List<String> trip) {
		ArrayList<String> tripDetails = new ArrayList<>();
		String pickupTime = trip.get(1);
		String pickupLocation = trip.get(7);
		tripDetails.add(pickupTime);
		tripDetails.add(pickupLocation);
		return tripDetails;
	}
	public void CheckAccidentInfo(String dateLocationInfo) {
		String dat[] = dateLocationInfo.split(" ");
		String date = dat[0];
		String time = dat[1];
		String boroughInfo = dat[2].split(",")[0].substring(1).toLowerCase();
		
		if(TestFile.mapAccidentInfo.containsKey(date) && TestFile.mapAccidentInfo.get(date).containsKey(time)
				&& TestFile.mapAccidentInfo.get(date).get(time).containsKey(boroughInfo)) {
			System.out.println("Accident happened on Date : " + date + " at time : " + time + " in borough : " + boroughInfo);
			System.out.println("Accident freq on this location : " + TestFile.mapAccidentInfo.get(date).get(time).get(boroughInfo).toString());  
		}
		
	}
	public void StoreFrequencyDayTumbling(ArrayList<String> tripData) {
		String date = tripData.get(0).split(" ")[0];
		String location = tripData.get(1);
		
		if(TestFile.mapDayTumbling.size()>0) {
			Entry<String, HashMap<String, ArrayList<ArrayList<String>>>> entry = 
					TestFile.mapDayTumbling.entrySet().iterator().next();
			String dateInMap = entry.getKey();
			if(!dateInMap.equals(date)) { // new date. Print analytics and flush the date in the hashmap
				CalculateBusiestLocationDayWise(date);
				TestFile.mapDayTumbling.remove(dateInMap);
			}
		}
		
		if(TestFile.mapDayTumbling.containsKey(date))
		{
			HashMap<String,ArrayList<ArrayList<String>>> locationMap = 
					TestFile.mapDayTumbling.get(date);
			
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
			TestFile.mapDayTumbling.put(date, locationMap);
		}
	}
	public int CalculateFrequencyDayTumblingForLocation(String dateTime,String locationData) {
		String date = dateTime.split(" ")[0];
		int frequency = -1;
		if(TestFile.mapDayTumbling.containsKey(date) && TestFile.mapDayTumbling.get(date).containsKey(locationData)) {
			System.out.println("Frequency of "+ locationData +" at this :" +dateTime);
			frequency = TestFile.mapDayTumbling.get(date).get(locationData).size();
		}
		else 
			System.out.println("No trips for this time and location");
		return frequency;
	}
	public void CalculateFrequencyDayTumbling(String dateTime) {
		String date = dateTime.split(" ")[0];
		if(TestFile.mapDayTumbling.containsKey(date)) {
			HashMap<String,ArrayList<ArrayList<String>>> locationMap = TestFile.mapDayTumbling.get(date);
			System.out.println("Frequency of all locations at this :" + date);
			locationMap.forEach((key,value) -> System.out.println(key + " -> " + value.size()));
		}
		else 
			System.out.println("No trips for this day");
	}
	public void StoreFrequencyHourTumbling(ArrayList<String> tripData) {
		
		String date = tripData.get(0).split(" ")[0];
		String hour = tripData.get(0).split(" ")[1].split(":")[0];
		String location = tripData.get(1);
		
		if(TestFile.mapHourTumbling.size()>0) {
			Entry<String, HashMap<String, HashMap<String, ArrayList<ArrayList<String>>>>> entry = 
					TestFile.mapHourTumbling.entrySet().iterator().next();
			String dateInMap = entry.getKey();
			if(!dateInMap.equals(date)) { // new date. Print analytics and flush the date in the hashmap
				ArrayList<String> busiestHours = CalculateBusiestHourDayWise(date);
				for(String busyHour : busiestHours) {
					ArrayList<String> busiestLocations = CalculateBusiestLocationHourWise(date,busyHour);
					for(String busyLocation : busiestLocations) {
						//send busy location, date, busy hour to a queue for accident analysis
						System.out.println("Test Print");
					}
				}
				TestFile.mapHourTumbling.remove(dateInMap);
			}
		}
		
		if(TestFile.mapHourTumbling.containsKey(date))
		{
			HashMap<String,HashMap<String,ArrayList<ArrayList<String>>>> hourMap = 
					TestFile.mapHourTumbling.get(date);
			if(hourMap.containsKey(hour)) {
				HashMap<String,ArrayList<ArrayList<String>>> locationMap = 
						hourMap.get(hour);
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
				hourMap.put(hour, locationMap);
			}
		}
		else {
			HashMap<String,ArrayList<ArrayList<String>>> locationMap = new HashMap<String,ArrayList<ArrayList<String>>>();
			ArrayList<ArrayList<String>> locationList=new ArrayList<>();
			locationList.add(tripData);
			locationMap.put(location, locationList);
			HashMap<String,HashMap<String,ArrayList<ArrayList<String>>>> hourMap = new HashMap<String,HashMap<String,ArrayList<ArrayList<String>>>>();
			
			hourMap.put(hour, locationMap);
			TestFile.mapHourTumbling.put(date, hourMap);
		}
	}
	public int CalculateFrequencyHourTumblingForLocation(String dateTime,String locationData) {
		String date = dateTime.split(" ")[0];
		String hour = dateTime.split(" ")[1].split(":")[0];
		int frequency = -1;
		if(TestFile.mapHourTumbling.containsKey(date) && TestFile.mapHourTumbling.get(date).containsKey(hour) &&
				TestFile.mapHourTumbling.get(date).get(hour).containsKey(locationData)) {
			System.out.println("Frequency of "+ locationData +" at this :" +dateTime);
			frequency = TestFile.mapHourTumbling.get(date).get(hour).get(locationData).size();
		}
		else 
			System.out.println("No trips for this time and location");
		return frequency;
	}
	public void CalculateFrequencyHourTumbling(String dateTime) {
		String date = dateTime.split(" ")[0];
		String hour = dateTime.split(" ")[1].split(":")[0];
		if(TestFile.mapHourTumbling.containsKey(date) && TestFile.mapHourTumbling.get(date).containsKey(hour)) {
			HashMap<String,ArrayList<ArrayList<String>>> locationMap = TestFile.mapHourTumbling.get(date).get(hour);
			System.out.println("Frequency of all locations at this :" +dateTime);
			locationMap.forEach((key,value) -> System.out.println(key + " -> " + value.size()));
		}
		else 
			System.out.println("No trips for this time");
	}
	public ArrayList<String> CalculateBusiestHourDayWise(String date) {
		HashMap<String,Integer> hourFreq = new HashMap<String,Integer>();
		if(TestFile.mapHourTumbling.containsKey(date)) {
			for (Entry<String, HashMap<String, ArrayList<ArrayList<String>>>> entry : TestFile.mapHourTumbling.get(date).entrySet()) {
			    //System.out.println(entry.getKey() + " = " + entry.getValue());
			    int freqCount = 0;
			    for(Entry<String,ArrayList<ArrayList<String>>> currentLoc : entry.getValue().entrySet()) {
			    	freqCount += currentLoc.getValue().size();
			    }
			    //Integer freq = new Integer(freqCount);
			    hourFreq.put(entry.getKey(), freqCount);
			}
		}
		Map<String, Integer> sortedMap = hourFreq.entrySet()
				.stream().sorted(Entry.comparingByValue(Comparator.reverseOrder()))
				.collect(Collectors.toMap(Entry::getKey, Entry::getValue,(e1, e2) -> e1, LinkedHashMap::new));
		//sortedMap.forEach((key,value) -> System.out.println(key + " -> " + value));
		int counter = 0;
		ArrayList<String> busiestHours = new ArrayList<>();
		System.out.println("Top 3 busiest hour in the day : " + date);
		for(Entry<String,Integer> locEntry : sortedMap.entrySet()) {
			if(counter<3) {
				System.out.println(locEntry.getKey() + " -> " + locEntry.getValue());
				busiestHours.add(locEntry.getKey());
			}
			else
				break;
			counter++;
		}
		return busiestHours;
	}
	public ArrayList<String> CalculateBusiestLocationHourWise(String date, String hour) {
		HashMap<String,Integer> locFreq = new HashMap<>();
		if(TestFile.mapHourTumbling.containsKey(date) && TestFile.mapHourTumbling.get(date).containsKey(hour)) {
			for(Entry<String,ArrayList<ArrayList<String>>> entry : TestFile.mapHourTumbling.get(date).get(hour).entrySet() ) {
				locFreq.put(entry.getKey(), entry.getValue().size());
			}
		}
		Map<String, Integer> sortedMap = locFreq.entrySet()
				.stream().sorted(Entry.comparingByValue(Comparator.reverseOrder()))
				.collect(Collectors.toMap(Entry::getKey, Entry::getValue,(e1, e2) -> e1, LinkedHashMap::new));
//		System.out.println("Test : ");
//		sortedMap.forEach((key,value) -> System.out.println(key + " -> " + value));
		int counter = 0;
		ArrayList<String> busiestLocations = new ArrayList<>();
		System.out.println("Top 3 busiest location in the day : " + date + " hour : " + hour);
		for(Entry<String,Integer> locEntry : sortedMap.entrySet()) {
			if(counter<3) {
				System.out.println(locEntry.getKey() + " -> " + locEntry.getValue());
				busiestLocations.add(locEntry.getKey());
			}
			else
				break;
			counter++;
		}
		return busiestLocations;
	}
	public void CalculateBusiestLocationDayWise(String dateTime) {
		String date = dateTime.split(" ")[0];
		HashMap<String,Integer> locFreq = new HashMap<String,Integer>();
		if(TestFile.mapDayTumbling.containsKey(date)) {
			for(Entry<String,ArrayList<ArrayList<String>>> entry : TestFile.mapDayTumbling.get(date).entrySet() ) {
				locFreq.put(entry.getKey(), entry.getValue().size());
			}
		}
		Map<String, Integer> sortedMap = locFreq.entrySet()
				.stream().sorted(Entry.comparingByValue(Comparator.reverseOrder()))
				.collect(Collectors.toMap(Entry::getKey, Entry::getValue,(e1, e2) -> e1, LinkedHashMap::new));
		//sortedMap.forEach((key,value) -> System.out.println(key + " -> " + value));
		int counter = 0;
		System.out.println("Top 3 busiest location in the day : " + dateTime);
		for(Entry<String,Integer> locEntry : sortedMap.entrySet()) {
			if(counter<3)
				System.out.println(locEntry.getKey() + " -> " + locEntry.getValue());
			else
				break;
			counter++;
		}
	}
	
	public void CreateLocationMap() throws IOException{
		TestFile.cashType.put("1", "Credit Card");
		TestFile.cashType.put("2", "Cash");
		TestFile.cashType.put("3", "No Charge");
		TestFile.cashType.put("4", "Dispute");
		TestFile.cashType.put("5", "Unknown");
		TestFile.cashType.put("6", "Voided Trip");
		BufferedReader csvReader = new BufferedReader(new FileReader("taxi+_zone_lookup.csv"));
		String row;
		boolean first=true;
		while ((row = csvReader.readLine()) != null) {
			if(first) {
				first = false;
			}
			else {
			String[] line = row.split(",");
			String boroughZone = "("+line[1]+","+line[2]+")";
			TestFile.locationIdMap.put(line[0], boroughZone);
			}
		}
		csvReader.close();
	}
	public void StoreAccidentDataMap(ArrayList<String> accidentData) {
		String date = accidentData.get(0);
		String time = accidentData.get(1);
		String location = accidentData.get(2).toLowerCase();
		
		if(TestFile.mapAccidentInfo.containsKey(date)) {
			HashMap<String,HashMap<String,Integer>> dateMap = TestFile.mapAccidentInfo.get(date);
			if(dateMap.containsKey(time)) {
				HashMap<String,Integer> hourMap = dateMap.get(time); 
				if(hourMap.containsKey(location)) {
					int freq = hourMap.get(location);
					hourMap.put(location, freq+1);
				}
				else {
					hourMap.put(location, 1);
				}
			}
			else {
				HashMap<String,Integer> hourMap = new HashMap<>();
				hourMap.put(location, 1);
				dateMap.put(time, hourMap);
			}
		}
		else {
			HashMap<String,HashMap<String,Integer>> dateMap = new HashMap<>();
			HashMap<String,Integer> hourMap = new HashMap<>();
			hourMap.put(location, 1);
			dateMap.put(time, hourMap);
			TestFile.mapAccidentInfo.put(date, dateMap);
		}
	}
	public void ReadAccidentData(String filename) throws IOException {
		BufferedReader csvFileReader = new BufferedReader(new FileReader(filename));
		String row;
		boolean first = true;
		
		while((row = csvFileReader.readLine()) != null) {
			if(first) {
				first = false;
			}
			else {
				if(!row.equals("")) {
					String data[] = row.split(",");
					if(!data[2].equals("")) {
						ArrayList<String> accidentData = new ArrayList<>();
						accidentData.add(data[0]);
						accidentData.add(data[1].split(":")[0]);
						accidentData.add(data[2]);
						StoreAccidentDataMap(accidentData);
					}
				}
			}
		}
		csvFileReader.close();
	}
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		String row;
		boolean first = true;
		TestFile test = new TestFile();
		test.CreateLocationMap();
		BufferedReader csvReader = new BufferedReader(new FileReader("testDat.csv"));
		while ((row = csvReader.readLine()) != null) {
			if(first) {
				first = false;
			}
			else {
				if(!row.equals("")) {	
				    String[] data = row.split(",");
				    List<String> currentTrip = test.PreProcessData(data);
				    ArrayList<String> tripDataCleaned = test.CleanData(currentTrip);
				    test.StoreFrequencyHourTumbling(tripDataCleaned);
				    test.StoreFrequencyDayTumbling(tripDataCleaned);
				    }
				}
			}
		csvReader.close();
		test.CalculateFrequencyHourTumbling("2018-01-01 00:08:26");
//		int freqHour = test.CalculateFrequencyHourTumblingForLocation("2018-01-01 00:08:26", "(\"Manhattan\",\"Yorkville East\")");  
//		System.out.println(freq);
		
		test.CalculateFrequencyDayTumbling("2018-01-01 00:08:26");
//		int freqDay = test.CalculateFrequencyDayTumblingForLocation("2018-01-01 00:08:26", "(\"Manhattan\",\"Yorkville East\")");  
//		System.out.println(freq);
		
		test.CalculateBusiestHourDayWise("2018-01-01 00:08:26");
		test.CalculateBusiestLocationDayWise("2018-01-01 00:08:26");
		//test.CalculateBusiestLocationHourWise("2018-01-01 00:08:26");
	}

}
