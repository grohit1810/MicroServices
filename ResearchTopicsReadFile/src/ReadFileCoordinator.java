public class ReadFileCoordinator {
	
	//main function to start ReadFile and DataEnrichment service in parallel
	public static void main (String[] args) throws Exception {
		thread(new NYCDataReader(), false);
		thread(new NYCDataEnricher(), false);
	}
	
	public static void thread(Runnable runnable, boolean daemon) {
		Thread brokerThread = new Thread(runnable);
		brokerThread.setDaemon(daemon);
		brokerThread.start();
	}
}
