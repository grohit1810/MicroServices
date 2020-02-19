


public class MainCoordinator {
	
	public static void main (String[] args) throws Exception
	{
//		thread(new Producer(), false);
//		//thread(new Producer(), false);
//        //thread(new Consumer(), false);
//        //thread(new Consumer(), false);
//        thread(new Consumer(), false);
//		Producer producer = new Producer();
//		Consumer consumer = new Consumer();
//
//		producer.run();
//		consumer.run();
		thread(new Producer(), false);
		thread(new Consumer(), false);		
   
	}
	
	public static void thread(Runnable runnable, boolean daemon) {
		
		Thread brokerThread = new Thread(runnable);
		brokerThread.setDaemon(daemon);
		brokerThread.start();
	}

}
