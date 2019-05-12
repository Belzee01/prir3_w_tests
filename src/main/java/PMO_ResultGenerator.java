
public class PMO_ResultGenerator {
	private static final long v1 = 1329305L;
	private static final long v2 = 390872898L;
	private static final long v3 = 123456;
	
	public static long result( long taskID ) {
		return (taskID * v1 + v2) % v3;
	}
}
