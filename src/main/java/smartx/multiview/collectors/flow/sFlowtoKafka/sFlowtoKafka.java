package smartx.multiview.collectors.flow.sFlowtoKafka;


import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Timestamp;
import java.util.Timer;
import java.util.TimerTask;

/**
*
* The Class File of OpenDayLightUtils.
*
* The class for querying inserting the flow entry to the OpenFlow Controller. 
* Developing by utilizing OpenDayLight REST API.
*
* @author Aris C. Risdianto
* @author GIST NetCS
* 
*/

public class sFlowtoKafka extends TimerTask {

	private String server;
	private String topic;
	
	public sFlowtoKafka(String server, String topic) {
		this.server = server;
		this.topic = topic;
	}
	
	 public void run()
    {

    String baseURL = "http://103.22.221.55:8008";
    String RESTAPI = "app/dashboard-example/scripts/metrics.js/metric/json";
    //String BoxName = "";

    
    /** Check the connection for ODP REST API */ 
    try {
        // Create URL = base URL + container
        URL url = new URL(baseURL + "/" + RESTAPI);
        // Create Http connection
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        // Set connection properties
        connection.setRequestMethod("GET");
        connection.setRequestProperty("Accept", "application/json");
        // Get the response from connection's inputStream
        InputStream content = (InputStream) connection.getInputStream();
        BufferedReader in = new BufferedReader(new InputStreamReader(content));
        
        String line = "";
		
        //String topic = "sFlow";
        
        
		String key = "sFlow";
		java.util.Date date= new java.util.Date();
		Timestamp now= new Timestamp(date.getTime());
		//System.out.println(now);
		
		//BoxName = InetAddress.getLocalHost().getHostName();
		
        String value = now+" = {\"TotalFlows\":[" ;
        
        /** Print line by line put into Kafka Value */ 
        while ((line = in.readLine()) != null) {
        	//System.out.println(line);
        	value = value+line;
        }
        
        System.out.println(value);
    	
        /** Open Kafka Consumer and Sent */ 
		try {
			KafkaSent.main(server, topic, key, value);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
        
    } catch (Exception e) {
        e.printStackTrace();
    }
    }

public static void main (String[] args)
	{
		
	//String topic = "sFlow";

	String server = args[0];
	String topic = args[1];
	
	//Scheduler every 10 seconds 
	Timer timer = new Timer();
	timer.schedule(new sFlowtoKafka(server, topic),0,10000);
		

		
	//	new sFlowtoKafka(topic).start();
	
	}

}

