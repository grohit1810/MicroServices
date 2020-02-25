import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.TextMessage;

public class AccidentAnalysisListener implements MessageListener {
	Connection consumerConnection;
	Session consumerSession;
	//Hashmap to store accident data look up table
	public static HashMap<String,HashMap<String,HashMap<String,Integer>>> mapAccidentInfo =
			new HashMap<>();
	public static BufferedWriter writer;
	//read the accident data file and store the accident data in the lookup table
	public AccidentAnalysisListener() throws IOException {
		writer = new BufferedWriter(new FileWriter("AccidentAnalysisResults.txt"));
		ReadAccidentData("Motor_Vehicle_Collisions.csv");
	}
	// this funciton stores accident data into a look up table(hashmap)
	public void StoreAccidentDataMap(ArrayList<String> accidentData) {
		String date = accidentData.get(0).trim();
		String time = accidentData.get(1).trim();
		String location = accidentData.get(2).trim().toLowerCase();
		
		if(AccidentAnalysisListener.mapAccidentInfo.containsKey(date)) {
			HashMap<String,HashMap<String,Integer>> dateMap = AccidentAnalysisListener.mapAccidentInfo.get(date);
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
			AccidentAnalysisListener.mapAccidentInfo.put(date, dateMap);
		}
	}
	//read the accident data file from filesystem
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
	
	@Override
	public void onMessage(Message msg) {
		try {
			
			if (msg instanceof TextMessage)
			{
				TextMessage textMessage = (TextMessage) msg;
				
				String text = textMessage.getText();
				if(text.equals("EXITCONNECTION")) {
					closeAllConnections();
					return;
				}
				CheckAccidentInfo(text);
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
	//this function checks for accident in the lookup table. This function also prints accident details into the console.
	public void CheckAccidentInfo(String dateLocationInfo) throws IOException {
		String dat[] = dateLocationInfo.split("\t");
		String date = dat[0];
		String time = dat[1];
		String boroughInfo = dat[2];
		if(AccidentAnalysisListener.mapAccidentInfo.containsKey(date) && AccidentAnalysisListener.mapAccidentInfo.get(date).containsKey(time)
				&& AccidentAnalysisListener.mapAccidentInfo.get(date).get(time).containsKey(boroughInfo)) {
			writer.write("Accident happened on Date : " + date + " at time : " + time + " in borough : " + boroughInfo + "\n");
			
			System.out.println("Accident happened on Date : " + date + " at time : " + time + " in borough : " + boroughInfo );
			System.out.println("Accident freq on this location : " + AccidentAnalysisListener.mapAccidentInfo.get(date).get(time).get(boroughInfo).toString()); 
			System.out.println("*************************************************************************************");
			writer.write("Accident freq on this location : " + AccidentAnalysisListener.mapAccidentInfo.get(date).get(time).get(boroughInfo).toString() + "\n"); 
			writer.write("*******************************************************************************\n");
		}
	}
	private void closeAllConnections() throws IOException, JMSException {
		consumerSession.close();
		consumerConnection.close();
		writer.close();
	}
	public void setConsumerObjectsToClose(Session session, Connection connection) {
		consumerSession = session;
		consumerConnection = connection;
		
	}

}
