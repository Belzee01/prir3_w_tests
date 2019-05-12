import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class PMO_TaskGenerator {
	private final static Set<Long> usedIDs = new HashSet<>();
	private final static Random rnd = new Random();
	
	/**
	 * Metoda zwraca zadanie z losowym, unikalnym numerem ID
	 * @return zadanie z unikalnym ID
	 */
	public synchronized static PMO_Task getTask() {
		long id;
		do {
			id = rnd.nextLong();
		} while ( usedIDs.contains( id ));
		usedIDs.add( id );
		return new PMO_Task( id );
	}
}
