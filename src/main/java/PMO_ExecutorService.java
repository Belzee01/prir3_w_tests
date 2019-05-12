import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class PMO_ExecutorService extends UnicastRemoteObject
		implements ExecutorServiceInterface, PMO_Testable, PMO_LogSource {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8176582092604542281L;
	private final int numberOfTasksAllowed;
	private final long slowDown;
	private final AtomicInteger numberOfConcurrentTasks;
	private volatile boolean nullDetected;
	private volatile boolean numberOfConcurrentTasksExceeded;
	private final Collection<Long> tasksReceived;
	private final int blockAfter; // kiedy blokada serwisu
	private final PMO_Barrier masterBlockade;
	private final PMO_Barrier thisServiceBlockade;
	private volatile TaskDispatcherInterface dispatcher;
	private volatile boolean taskSubmissionTestEnabled;
	private volatile boolean taskSubmissionTestVerify;
	private volatile boolean taskSubmissionTestStarted;
	private final AtomicBoolean taskSubmissionTestPassed;
	private volatile PMO_Task expectedAsNext;
	private volatile String executorServiceName;
	private volatile boolean newTaskSubmissionPriority;
	private volatile boolean priorityTestVerify;
	private volatile boolean priorityTestStarted;
	private volatile boolean priorityTestPassed;

	public PMO_ExecutorService(int numberOfTasksAllowed, long slowDown, int blockAfter, PMO_Barrier thisServiceBlockade,
			PMO_Barrier masterBlockade) throws RemoteException {
		this.blockAfter = blockAfter;
		this.numberOfTasksAllowed = numberOfTasksAllowed;
		this.slowDown = slowDown;
		this.masterBlockade = masterBlockade;
		this.thisServiceBlockade = thisServiceBlockade;
		this.taskSubmissionTestPassed = new AtomicBoolean();
		numberOfConcurrentTasks = new AtomicInteger(0);
		tasksReceived = new ConcurrentLinkedQueue<Long>();
	}

	public void taskSubmissionTestPrepare(TaskDispatcherInterface dispatcher, String executorServiceName,
			boolean newTaskSubmissionPriority) {
		this.dispatcher = dispatcher;
		this.executorServiceName = executorServiceName;
		this.newTaskSubmissionPriority = newTaskSubmissionPriority;
		taskSubmissionTestEnabled = true;
		taskSubmissionTestVerify = true;
	}

	public static PMO_ExecutorService create(int numberOfTasksAllowed, long slowDown, int blockAfter,
			PMO_Barrier thisServiceBlockade, PMO_Barrier masterBlockade) {
		PMO_ExecutorService exec = null;

		try {
			exec = new PMO_ExecutorService(numberOfTasksAllowed, slowDown, blockAfter, thisServiceBlockade,
					masterBlockade);
		} catch (RemoteException e) {
			e.printStackTrace();
			System.exit(0);
		}

		return exec;
	}

	@Override
	public int numberOfTasksAllowed() throws RemoteException {
		return numberOfTasksAllowed;
	}

	@Override
	public long execute(TaskInterface task) throws RemoteException {

		int numberOfConcurrentTasksNow = numberOfConcurrentTasks.incrementAndGet();
		if (numberOfConcurrentTasksNow > numberOfTasksAllowed) {
			numberOfConcurrentTasksExceeded = true;
			log("Wykryto przekroczenie dozwolonej liczby współbieżnych zadań. Powinno być " + numberOfTasksAllowed
					+ " a jest " + numberOfConcurrentTasksNow);
		}

		if (expectedAsNext != null) {
			priorityTestStarted = true;
			if (task.taskID() == expectedAsNext.taskID()) {
				priorityTestPassed = true;
				log("Zaliczono test priorytetyzacji zadań, otrzymano zadanie " + task.taskID());
			} else {
				log("Test priorytetyzacji nie zaliczony. Oczekiwano " + expectedAsNext.taskID() + " otrzymano "
						+ task.taskID());
			}

			expectedAsNext = null;
		}

		if (masterBlockade != null)
			masterBlockade.await();
		if (thisServiceBlockade != null)
			thisServiceBlockade.await();

		PMO_TimeHelper.sleep(slowDown);

		if (task == null) {
			log("Metodę execute wykonano z argumentem null");
			nullDetected = true;
			return -1;
		}

		int numberOfTasksReceived;
		synchronized (tasksReceived) {
			tasksReceived.add(task.taskID());
			numberOfTasksReceived = tasksReceived.size();
		}

		if (numberOfTasksReceived > blockAfter) { // zablokowanie serwisu po wykonaniu określonej pracy
			PMO_TimeHelper.sleep(PMO_Consts.BLOCKING_FOREVER);
		}

		long result = PMO_ResultGenerator.result(task.taskID());

		numberOfConcurrentTasks.decrementAndGet();

		if (taskSubmissionTestEnabled) {
			taskSubmissionTestEnabled = false;
			log("Rozpoczęto test wprowadzania nowego zadania w trakcie obliczeń");
			PMO_Task newTask = PMO_TaskGenerator.getTask();
			log("Wygenerowano nowe zadanie " + newTask.taskID());
			if (newTaskSubmissionPriority) {
				expectedAsNext = newTask;
				priorityTestVerify = true;
			}
			Thread th = new Thread(() -> {
				try {
					dispatcher.addTask(newTask, executorServiceName, newTaskSubmissionPriority);
					taskSubmissionTestPassed.set(true);
					log("Dodatkowy wątek wprowadzający nowe zadanie wykonał pracę");
				} catch (RemoteException e) {
					e.printStackTrace();
				}
			});
			th.setDaemon(true);
			th.start();
			log("Uruchomiono dodatkowy wątek wprowadzający nowe zadanie");
			taskSubmissionTestStarted = true;
			try {
				th.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		return result;
	}

	@Override
	public void test() {
		assertFalse("Zamiast obiektu do execute dostarczono null", nullDetected);
		assertFalse("Wykryto przekroczenie liczby dozwolonych, współbieżnych wątków", numberOfConcurrentTasksExceeded);
		if (taskSubmissionTestVerify) {
			assertTrue(
					"Nie wykryto uruchomienia testu współbieżności wprowadzania zadań - nie wykonano metody execute?",
					taskSubmissionTestStarted);
			assertTrue("Test wprowadzania zadań w trakcie obliczeń nie został zaliczony",
					taskSubmissionTestPassed.get());
		}
		if (priorityTestVerify) {
			assertTrue("Nie wykryto uruchomienia testu priorytetyzacji", priorityTestStarted);
			assertTrue("Test priorytetyzacji nie został zaliczony", priorityTestPassed);
		}
	}

	public Collection<Long> getTaskIDs() {
		return tasksReceived;
	}

}
