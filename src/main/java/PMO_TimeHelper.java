
public class PMO_TimeHelper {
	
	private static final long startAt = getMsec();
	
	public static long timeFromStart() {
		return getMsec() - startAt;
	}
	
	public static boolean interruptableSleep( long msec ) {
		try {
			Thread.sleep( msec );
		} catch ( Exception e ) {
			return false;
		}
		return true;
	}
	
	public static long getMsec() {
		return System.nanoTime() / 1000000;
	}
	
	public static boolean uninterruptibleSleep( long msec ) {
		long startAt = getMsec();
		boolean interruptionDetected = false;
		long delta;
		while ( true ) {
			delta = getMsec() - startAt;
			
			if ( delta < msec ) {
				if ( ! interruptableSleep( msec - delta ) ) 
					interruptionDetected = true;
			} else 
				return interruptionDetected;
		}
	}
	
	public static void sleep( long msec ) {
		uninterruptibleSleep(msec);
	}
	
	public static void burnCPU( long msec, boolean yield ) {
		long delta;
		long startAt = getMsec();
		while ( true ) {
			delta = getMsec() - startAt;
			
			if ( delta < msec ) {
				if ( yield ) Thread.yield();
			} else {
				return;
			}
		}		
	}
	
	public static void main(String[] args) {
		System.out.println( "Start@ " + getMsec() );
		burnCPU(1000,true);
		burnCPU(1000,false);
		sleep(1000);
		System.out.println( "End@   " + getMsec() );
		
	}
}

