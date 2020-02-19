import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;

public class AccidentAnalysisListener implements MessageListener {
	Connection consumerConnection;
	public static HashMap<String,HashMap<String,HashMap<String,Integer>>> mapAccidentInfo =
			new HashMap<>();
	public static BufferedWriter writer;
	public AccidentAnalysisListener() throws IOException {
		writer = new BufferedWriter(new FileWriter("AccidentAnalysisResults.txt"));
		ReadAccidentData("Motor_Vehicle_Collisions.csv");
	}
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
								
				//System.out.println("Received: " + text);
				/*
				 * String[] accidentCheckInfo = text.split(" ");
				 * //System.out.println(cleanedTrip[0]); ArrayList<String> accidentDetailsList =
				 * new ArrayList<>(); for(int i=0;i<accidentCheckInfo.length;i++)
				 * accidentDetailsList.add(accidentCheckInfo[i]);
				 */
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
	public void CheckAccidentInfo(String dateLocationInfo) throws IOException {
		String dat[] = dateLocationInfo.split("\t");
		String date = dat[0];
		String time = dat[1];
		/*
		 * String boroughInfo = dat[2].split(",")[0].substring(1); boroughInfo =
		 * boroughInfo.substring(1,boroughInfo.length()-1).toLowerCase();
		 */
		String boroughInfo = dat[2];
//		System.out.println("Data received in accident, Date:" + date + " Time: " + time + " Borough: " + boroughInfo);
//		System.out.println(AccidentAnalysisListener.mapAccidentInfo.size());
		if(AccidentAnalysisListener.mapAccidentInfo.containsKey(date) && AccidentAnalysisListener.mapAccidentInfo.get(date).containsKey(time)
				&& AccidentAnalysisListener.mapAccidentInfo.get(date).get(time).containsKey(boroughInfo)) {
			writer.write("Accident happened on Date : " + date + " at time : " + time + " in borough : " + boroughInfo + "\n");
			System.out.println("Accident happened on Date : " + date + " at time : " + time + " in borough : " + boroughInfo );
			writer.write("Accident freq on this location : " + AccidentAnalysisListener.mapAccidentInfo.get(date).get(time).get(boroughInfo).toString() + "\n"); 
			writer.write("*******************************************************************************\n");
		}
		
	}

	private void closeAllConnections() throws IOException {
		// TODO Auto-generated method stub
		writer.close();
	}
	public void setConnectionObjectToClose(Connection connection) {
		this.consumerConnection = connection;
	}

}
