import static org.junit.Assert.assertFalse;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class PMO_Receiver extends UnicastRemoteObject implements ReceiverInterface, PMO_Testable, PMO_LogSource {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1874381738215601256L;
	private final long slowDown;
	private volatile boolean parallelUsageError;
	private volatile boolean wrongResult;
	private final AtomicBoolean inUsage;
	private final Collection<Long> tasksReceived;
	private volatile boolean firstUsage = true;
	private volatile long firstUsageAt;
	private volatile long lastUsageAt;
	private final int blockAfter; // automatyczna blokada serwisu
	private final PMO_Barrier masterBlockade;
	private final PMO_Barrier thisServiceBlockade;

	/**
	 * Konstruktor serwisu odbierania wyników obliczeń
	 * 
	 * @param slowDown       każdorazowe opóźnienie w pracy
	 * @param blockAfter     blokada serwisu po odbiorze blockAfter wyników
	 * @param masterBlockade główna bariera wyłączająca serwis
	 * @throws RemoteException
	 */
	public PMO_Receiver(long slowDown, int blockAfter, PMO_Barrier thisServiceBlockade, PMO_Barrier masterBlockade)
			throws RemoteException {
		this.slowDown = slowDown;
		this.blockAfter = blockAfter;
		this.masterBlockade = masterBlockade;
		this.thisServiceBlockade = thisServiceBlockade;
		inUsage = new AtomicBoolean(false);
		tasksReceived = new ConcurrentLinkedQueue<>();
	}

	public static PMO_Receiver create(long slowDown, int blockAfter, PMO_Barrier thisServiceBlockade,
			PMO_Barrier masterBlockade) {
		PMO_Receiver receiver = null;

		try {
			receiver = new PMO_Receiver(slowDown, blockAfter, thisServiceBlockade, masterBlockade);
		} catch (RemoteException e) {
			e.printStackTrace();
			System.exit(0);
		}

		return receiver;

	}

	@Override
	public void result(long taskID, long result) throws RemoteException {

		if (!inUsage.compareAndSet(false, true)) {
			log("Wykryto, że metoda result wykonywana jest współbieżnie" );
			parallelUsageError = true;
		}

		if (masterBlockade != null)
			masterBlockade.await();
		if (thisServiceBlockade != null)
			thisServiceBlockade.await();

		if (result != PMO_ResultGenerator.result(taskID)) {
			log("Dostarczono błędny rezultat dla taskID = " + taskID + " powinno być "
					+ PMO_ResultGenerator.result(taskID) + " a jest " + result);
			wrongResult = true;
		}

		if (firstUsage) {
			firstUsage = false;
			firstUsageAt = PMO_TimeHelper.getMsec();
		} else {
			lastUsageAt = PMO_TimeHelper.getMsec();
		}

		PMO_TimeHelper.sleep(slowDown);

		int numberOfTasksReceived;
		synchronized (tasksReceived) {
			tasksReceived.add(taskID);
			numberOfTasksReceived = tasksReceived.size();
		}

		log( "Odebrano wynik zadania " + taskID + " " + numberOfTasksReceived );
		
		if (numberOfTasksReceived > blockAfter) { // zablokowanie serwisu po wykonaniu określonej pracy
			log( "Serwis odbierający zadania ulegnie teraz zablokowaniu" );
			PMO_TimeHelper.sleep(PMO_Consts.BLOCKING_FOREVER);
		}

		inUsage.set(false);
	}

	@Override
	public void test() {
		assertFalse("Do serwisu Receiver nie dotarł żaden rezultat", firstUsage);
		assertFalse("Wykryto współbieżne użycie metody result()", parallelUsageError);
		assertFalse("Przekazano błędny rezultat obliczeń", wrongResult);
	}

	public Collection<Long> getTaskIDs() {
		return new ArrayList<Long>(tasksReceived);
	}

	public long getWorkTime() {
		return lastUsageAt - firstUsageAt;
	}
}
