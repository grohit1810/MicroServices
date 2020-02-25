
import java.io.BufferedReader;
import java.io.FileReader;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;

import org.json.simple.JSONObject;

public class NYCDataReader implements Runnable {
	ActiveMQConnectionFactory connectionFactory;
	Connection connection;
	Session session;
	Destination dest;
	MessageProducer producer;
	
	//constructor to initialize ActiveMQ objects
	public NYCDataReader() throws JMSException {
		connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
		connection = connectionFactory.createConnection();
		connection.start();
		session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		dest = session.createQueue("StraightFromFileDataQueue");
		producer = session.createProducer(dest);
		producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
	}
	// read nyc data line by line, add the data into a queue.
	public void run() {
		try {
			BufferedReader br = new BufferedReader(new FileReader("yellow_tripdata_2018-01_mini_sample.csv"));
    	    String line;
    	    //JSONObject message1 = new JSONObject();
    	    //SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
    	    String headerLine = br.readLine();
    		while ((line = br.readLine()) != null) {
    			TextMessage message = session.createTextMessage(line);
    			producer.send(message);
    	    }
			br.close();
    		TextMessage finalMsg = session.createTextMessage("EXITCONNECTION");
    		producer.send(finalMsg);
    		this.producer.close();
    		this.session.close();
    		this.connection.close();
		}
		catch (Exception e) {
			e.printStackTrace();
		} 
	}
}
