import org.junit.jupiter.api.*;
import org.junit.jupiter.api.function.ThrowingSupplier;

import java.io.IOException;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.*;

public class PMO_Test implements PMO_LogSource {
	//////////////////////////////////////////////////////////////////////////
	private static final Map<String, Double> tariff = new HashMap<>();

	@Retention(RetentionPolicy.RUNTIME)
	public static @interface Tariff {
		double value();
	}

	static {
		tariff.put("normalWorkTest", 3.0);
		tariff.put("normalWorkTestMultipleExecutors", 3.0);
		tariff.put("concurrentTaskInsertTest", 3.0);
		tariff.put("priorityTest", 3.0);
		tariff.put("idependentTaskExecutionTest", 3.0);
		tariff.put("idependentTaskExecutionTest2", 3.0);
	}

	public static double getTariff(String testName) {
		return tariff.get(testName);
	}

	//////////////////////////////////////////////////////////////////////////

	private static void showException(Throwable e, String txt) {
		e.printStackTrace();
		fail("W trakcie pracy metody " + txt + " doszło do wyjątku " + e.toString());
	}

	////////////////////////////////////////////////////////////////////////////////

	private static <T> T tryToExecute(ThrowingSupplier<T> run, String txt, long timeLimit) {
		try {
			return assertTimeoutPreemptively(Duration.ofMillis(timeLimit), run::get, "Przekroczono limit czasu " + txt);
		} catch (Exception e) {
			showException(e, txt);
		}
		return null;
	}

	private static void tryToExecute(Runnable run, String txt, long timeLimit) {
		try {
			assertTimeoutPreemptively(Duration.ofMillis(timeLimit), run::run, "Przekroczono limit czasu " + txt);
		} catch (Exception e) {
			showException(e, txt);
		}
	}

	private Process rmireg;
	private Process execService;
	private TaskDispatcherInterface dispatcher;

	@BeforeEach
	public void create() {
		try {
			rmireg = PMO_ProcessHelper.create("rmiregistry");
		} catch (IOException e) {
			fail("Internal error - nie udało się uruchomić rmiregistry");
		}
		PMO_TimeHelper.sleep(2000);
		try {
			execService = PMO_ProcessHelper.create("java", "Start");
		} catch (IOException e) {
			fail("Nie udało się wykonać java Start");
		}
		PMO_TimeHelper.sleep(2000);

		Remote remote = PMO_RMIHelper.connect("TaskDispatcher");
		assertNotNull(remote, "Nie udało się podłączyć do serwisu TaskDispatcher");

		dispatcher = (TaskDispatcherInterface) remote;

		System.err.println(dispatcher);
	}

	private boolean testEquivalence(Collection<Long> expected, Collection<Long> actual) {

		if (expected.size() != actual.size()) {
			log("Oczekiwano " + expected.size() + " odebrano " + actual.size());
			return false;
		}

		if (!expected.containsAll(actual))
			return false;
		if (!actual.containsAll(expected))
			return false;
		return true;
	}

	private boolean repetitionsDetected(Collection<Long> c) {
		Set<Long> cSet = new TreeSet<Long>(c);

		if (c.size() != cSet.size())
			return true;
		return false;
	}

	@AfterEach
	public void shutdown() {
		PMO_ProcessHelper.kill(execService);
		PMO_ProcessHelper.kill(rmireg);
		PMO_ProcessHelper.childrenInfo();
	}

	@RepeatedTest(PMO_Consts.TEST_REPETITIONS)
//	@Disabled
	public void normalWorkTest() {
		log("normalWorkTest");
		// utworzyc wlasne serwisy

		// ES może działać wolniej od receivera, bo przetwarza wiele zadań jednocześnie
		long executorServiceSlowdown = PMO_Consts.NORMAL_WORK_RECEIVER_SLOWDOWN * PMO_Consts.NORMAL_WORK_TASKS_ALLOWED;

		long numberOfTasks = PMO_Consts.NORMAL_WORK_TASKS_PER_SENDER * PMO_Consts.NORMAL_WORK_SENDERS_PER_EXECUTOR;
		long requredTimeEstimation = numberOfTasks * PMO_Consts.NORMAL_WORK_RECEIVER_SLOWDOWN + executorServiceSlowdown;
		requredTimeEstimation = (long) (requredTimeEstimation * PMO_Consts.NORMAL_WORK_TIMECORRECTION);

		log("Number of tasks = " + numberOfTasks);
		log("Time required   = " + requredTimeEstimation);

		PMO_Barrier masterBlockade = new PMO_Barrier(PMO_Consts.NORMAL_WORK_TASKS_ALLOWED + 1, false, true, true,
				"master");
		PMO_Barrier executorBlockade = new PMO_Barrier(PMO_Consts.NORMAL_WORK_TASKS_ALLOWED, true, true, true,
				"executor");

		PMO_Receiver receiver = PMO_Receiver.create(PMO_Consts.NORMAL_WORK_RECEIVER_SLOWDOWN, PMO_Consts.BLOCK_NEVER,
				null, masterBlockade);

		PMO_RMIHelper.bind(receiver, PMO_Consts.RECEIVER_SERVICE_NAME);
		// przekazac info o nazwie receivera

		try {
			dispatcher.setReceiverServiceName(PMO_Consts.RECEIVER_SERVICE_NAME);
		} catch (RemoteException e) {
			e.printStackTrace();
			fail("W trakcie metody setReceiverServiceName doszlo do wyjatku " + e.toString());
		}

		PMO_ExecutorService exec = PMO_ExecutorService.create(PMO_Consts.NORMAL_WORK_TASKS_ALLOWED,
				executorServiceSlowdown, PMO_Consts.BLOCK_NEVER, executorBlockade, masterBlockade);

		PMO_RMIHelper.bind(exec, PMO_Consts.EXECUTOR_SERVICE_NAMES.get(0));

		// przygotowac zadania
		PMO_TaskSendersTeam team = new PMO_TaskSendersTeam(dispatcher, PMO_Consts.NORMAL_WORK_SENDERS_PER_EXECUTOR,
				PMO_Consts.NORMAL_WORK_TASKS_PER_SENDER, PMO_Consts.EXECUTOR_SERVICE_NAMES.get(0));

		// uruchomic transfer zadan
		team.start();

		// odczekać na dostarczenie zadań
		PMO_TimeHelper.sleep(2000);

		// odblokować pracę serwisów
		executorBlockade.trigger(true);

		PMO_TimeHelper.sleep(requredTimeEstimation);

		masterBlockade.enable(true);

		// sprawdzic poprawnosc pracy

		team.test();
		exec.test();
		receiver.test();

		// porównać rozwiązania

		Collection<Long> tasksSended = team.getTaskIDs();
		Collection<Long> tasksExecuted = exec.getTaskIDs();
		Collection<Long> tasksReceived = receiver.getTaskIDs();

		assertFalse(repetitionsDetected(tasksExecuted), "Wykryto powtórzenia identyfikatorów przetwarzanych zadań");
		assertFalse(repetitionsDetected(tasksReceived), "Wykryto powtórzenia identyfikatorów dostarczonych wyników");
		assertTrue(testEquivalence(tasksSended, tasksExecuted),
				"Wykryto rozbieżność pomiędzy zadaniami wysłanymi a przetworzonymi");
		assertTrue(testEquivalence(tasksSended, tasksReceived),
				"Wykryto rozbieżność pomiędzy zadaniami wysłanymi a odebranymi wynikami");
	}

	@RepeatedTest(PMO_Consts.TEST_REPETITIONS)
//	@Disabled
	public void normalWorkTestMultipleExecutors() {
		log("normalWorkTestMultipleExecutors");
		// utworzyc wlasne serwisy

		// ES może działać wolniej od receivera, bo przetwarza wiele zadań jednocześnie
		long executorServiceSlowdown = PMO_Consts.NORMAL_WORK_RECEIVER_SLOWDOWN
				* PMO_Consts.NORMAL_WORK_MULTIPLE_TASKS_ALLOWED * PMO_Consts.NORMAL_WORK_MULTIPLE_EXECUTORS;

		long numberOfTasks = PMO_Consts.NORMAL_WORK_MULTIPLE_TASKS_PER_SENDER
				* PMO_Consts.NORMAL_WORK_MULTIPLE_SENDERS_PER_EXECUTOR * PMO_Consts.NORMAL_WORK_MULTIPLE_EXECUTORS;
		long requredTimeEstimation = numberOfTasks * PMO_Consts.NORMAL_WORK_RECEIVER_SLOWDOWN + executorServiceSlowdown;
		requredTimeEstimation = (long) (requredTimeEstimation * PMO_Consts.NORMAL_WORK_TIMECORRECTION);

		log("Number of tasks           = " + numberOfTasks);
		log("Time required             = " + requredTimeEstimation);
		log("Executor service slowdown = " + executorServiceSlowdown);

		int concurrentTasks = PMO_Consts.NORMAL_WORK_MULTIPLE_TASKS_ALLOWED * PMO_Consts.NORMAL_WORK_MULTIPLE_EXECUTORS;
		log("Concurrent tasks          = " + concurrentTasks);

		PMO_Barrier masterBlockade = new PMO_Barrier(concurrentTasks + 1, false, true, true, "master");
		PMO_Barrier executorBlockade = new PMO_Barrier(concurrentTasks, true, true, true, "executor");

		PMO_Receiver receiver = PMO_Receiver.create(PMO_Consts.NORMAL_WORK_RECEIVER_SLOWDOWN, PMO_Consts.BLOCK_NEVER,
				null, masterBlockade);

		PMO_RMIHelper.bind(receiver, PMO_Consts.RECEIVER_SERVICE_NAME);

		// przekazac info o nazwie receivera
		try {
			dispatcher.setReceiverServiceName(PMO_Consts.RECEIVER_SERVICE_NAME);
		} catch (RemoteException e) {
			e.printStackTrace();
			fail("W trakcie metody setReceiverServiceName doszlo do wyjatku " + e.toString());
		}

		List<PMO_ExecutorService> executors = new ArrayList<>();
		String execNames[] = new String[PMO_Consts.NORMAL_WORK_MULTIPLE_EXECUTORS];

		for (int i = 0; i < PMO_Consts.NORMAL_WORK_MULTIPLE_EXECUTORS; i++) {
			PMO_ExecutorService exec = PMO_ExecutorService.create(PMO_Consts.NORMAL_WORK_MULTIPLE_TASKS_ALLOWED,
					executorServiceSlowdown, PMO_Consts.BLOCK_NEVER, executorBlockade, masterBlockade);

			PMO_RMIHelper.bind(exec, PMO_Consts.EXECUTOR_SERVICE_NAMES.get(i));

			executors.add(exec);
			execNames[i] = PMO_Consts.EXECUTOR_SERVICE_NAMES.get(i);
		}

		// przygotowac zadania
		PMO_TaskSendersTeam team = new PMO_TaskSendersTeam(dispatcher,
				PMO_Consts.NORMAL_WORK_MULTIPLE_SENDERS_PER_EXECUTOR, PMO_Consts.NORMAL_WORK_MULTIPLE_TASKS_PER_SENDER,
				execNames);

		// uruchomic transfer zadan
		team.start();

		// odczekać na dostarczenie zadań
		PMO_TimeHelper.sleep(2000);

		// odblokować pracę serwisów
		executorBlockade.trigger(true);

		PMO_TimeHelper.sleep(requredTimeEstimation);

		masterBlockade.enable(true);

		// sprawdzic poprawnosc pracy

		team.test();
		executors.forEach(e -> e.test());
		receiver.test();

		// porównać rozwiązania

		Collection<Long> tasksSended = team.getTaskIDs();
		Collection<Long> tasksExecuted = new ArrayList<Long>();
		Collection<Long> tasksReceived = receiver.getTaskIDs();

		executors.forEach(e -> tasksExecuted.addAll(e.getTaskIDs()));

		assertFalse(repetitionsDetected(tasksExecuted), "Wykryto powtórzenia identyfikatorów przetwarzanych zadań");
		assertFalse(repetitionsDetected(tasksReceived), "Wykryto powtórzenia identyfikatorów dostarczonych wyników");
		assertTrue(testEquivalence(tasksSended, tasksExecuted),
				"Wykryto rozbieżność pomiędzy zadaniami wysłanymi a przetworzonymi");
		assertTrue(testEquivalence(tasksSended, tasksReceived),
				"Wykryto rozbieżność pomiędzy zadaniami wysłanymi a odebranymi wynikami");
	}

	@RepeatedTest(PMO_Consts.TEST_REPETITIONS)
	public void idependentTaskExecutionTest() {
		log("idependentTaskExecutionTest");
		// utworzyc wlasne serwisy

		// ES może działać wolniej od receivera, bo przetwarza wiele zadań jednocześnie
		long executorServiceSlowdown = PMO_Consts.NORMAL_WORK_RECEIVER_SLOWDOWN
				* PMO_Consts.NORMAL_WORK_MULTIPLE_TASKS_ALLOWED * PMO_Consts.NORMAL_WORK_MULTIPLE_EXECUTORS;

		long numberOfTasks = PMO_Consts.NORMAL_WORK_MULTIPLE_TASKS_PER_SENDER
				* PMO_Consts.NORMAL_WORK_MULTIPLE_SENDERS_PER_EXECUTOR * PMO_Consts.NORMAL_WORK_MULTIPLE_EXECUTORS;
		long requredTimeEstimation = numberOfTasks * PMO_Consts.NORMAL_WORK_RECEIVER_SLOWDOWN + executorServiceSlowdown;
		requredTimeEstimation = (long) (requredTimeEstimation * PMO_Consts.NORMAL_WORK_TIMECORRECTION);

		int concurrentTasks = PMO_Consts.NORMAL_WORK_MULTIPLE_TASKS_ALLOWED * PMO_Consts.NORMAL_WORK_MULTIPLE_EXECUTORS;

		log("Number of tasks           = " + numberOfTasks);
		log("Time required             = " + requredTimeEstimation);
		log("Executor service slowdown = " + executorServiceSlowdown);
		log("Concurrent tasks          = " + concurrentTasks);

		PMO_Barrier masterBlockade = new PMO_Barrier(concurrentTasks + 1, false, true, true, "master");
		PMO_Barrier executorBlockade = new PMO_Barrier(concurrentTasks, true, true, true, "executor");

		PMO_Receiver receiver = PMO_Receiver.create(PMO_Consts.NORMAL_WORK_RECEIVER_SLOWDOWN,
				PMO_Consts.INDEPENDENT_TASK_EXECUTION_RECEIVER_BLOCKADE_AFTER, null, masterBlockade);

		PMO_RMIHelper.bind(receiver, PMO_Consts.RECEIVER_SERVICE_NAME);

		// przekazac info o nazwie receivera
		try {
			dispatcher.setReceiverServiceName(PMO_Consts.RECEIVER_SERVICE_NAME);
		} catch (RemoteException e) {
			e.printStackTrace();
			fail("W trakcie metody setReceiverServiceName doszlo do wyjatku " + e.toString());
		}

		List<PMO_ExecutorService> executors = new ArrayList<>();
		String execNames[] = new String[PMO_Consts.NORMAL_WORK_MULTIPLE_EXECUTORS];

		for (int i = 0; i < PMO_Consts.NORMAL_WORK_MULTIPLE_EXECUTORS; i++) {
			PMO_ExecutorService exec = PMO_ExecutorService.create(PMO_Consts.NORMAL_WORK_MULTIPLE_TASKS_ALLOWED,
					executorServiceSlowdown, PMO_Consts.BLOCK_NEVER, executorBlockade, masterBlockade);

			PMO_RMIHelper.bind(exec, PMO_Consts.EXECUTOR_SERVICE_NAMES.get(i));

			executors.add(exec);
			execNames[i] = PMO_Consts.EXECUTOR_SERVICE_NAMES.get(i);
		}

		// przygotowac zadania
		PMO_TaskSendersTeam team = new PMO_TaskSendersTeam(dispatcher,
				PMO_Consts.NORMAL_WORK_MULTIPLE_SENDERS_PER_EXECUTOR, PMO_Consts.NORMAL_WORK_MULTIPLE_TASKS_PER_SENDER,
				execNames);

		// uruchomic transfer zadan
		team.start();

		// odczekać na dostarczenie zadań
		PMO_TimeHelper.sleep(2000);

		// odblokować pracę serwisów
		executorBlockade.trigger(true);

		PMO_TimeHelper.sleep(requredTimeEstimation);

		masterBlockade.enable(true);

		Collection<Long> tasksSended = team.getTaskIDs();
		Collection<Long> tasksExecuted = new ArrayList<Long>();
		Collection<Long> tasksReceived = receiver.getTaskIDs();

		executors.forEach(e -> tasksExecuted.addAll(e.getTaskIDs()));

		assertEquals(PMO_Consts.INDEPENDENT_TASK_EXECUTION_RECEIVER_BLOCKADE_AFTER + 1, tasksReceived.size(),
				"Nie wszystkie wyniki zadań zostały dostarczone");
		assertTrue(testEquivalence(tasksSended, tasksExecuted), "Nie wszystkie zadania zostały przetworzone");
	}

	@RepeatedTest(PMO_Consts.TEST_REPETITIONS)
	public void idependentTaskExecutionTest2() {
		log("idependentTaskExecutionTest2");
		// utworzyc wlasne serwisy

		long numberOfTasks = PMO_Consts.NORMAL_WORK_MULTIPLE_TASKS_PER_SENDER
				* PMO_Consts.NORMAL_WORK_MULTIPLE_SENDERS_PER_EXECUTOR * PMO_Consts.NORMAL_WORK_MULTIPLE_EXECUTORS;
		long requredTimeEstimation = 3000;

		int concurrentTasks = PMO_Consts.NORMAL_WORK_MULTIPLE_TASKS_ALLOWED * PMO_Consts.NORMAL_WORK_MULTIPLE_EXECUTORS;

		log("Number of tasks           = " + numberOfTasks);
		log("Time required             = " + requredTimeEstimation);
		log("Concurrent tasks          = " + concurrentTasks);

		PMO_Barrier masterBlockade = new PMO_Barrier(concurrentTasks + 1, false, true, true, "master");
		PMO_Barrier executorBlockade = new PMO_Barrier(concurrentTasks, true, true, true, "executor");
		PMO_Barrier receiverBlockade = new PMO_Barrier(1, true, true, true, "receiver");

		PMO_Receiver receiver = PMO_Receiver.create(0, PMO_Consts.BLOCK_NEVER, receiverBlockade, masterBlockade);

		PMO_RMIHelper.bind(receiver, PMO_Consts.RECEIVER_SERVICE_NAME);

		// przekazac info o nazwie receivera
		try {
			dispatcher.setReceiverServiceName(PMO_Consts.RECEIVER_SERVICE_NAME);
		} catch (RemoteException e) {
			e.printStackTrace();
			fail("W trakcie metody setReceiverServiceName doszlo do wyjatku " + e.toString());
		}

		List<PMO_ExecutorService> executors = new ArrayList<>();
		String execNames[] = new String[PMO_Consts.NORMAL_WORK_MULTIPLE_EXECUTORS];

		for (int i = 0; i < PMO_Consts.NORMAL_WORK_MULTIPLE_EXECUTORS; i++) {
			PMO_ExecutorService exec = PMO_ExecutorService.create(PMO_Consts.NORMAL_WORK_MULTIPLE_TASKS_ALLOWED, 0,
					PMO_Consts.BLOCK_NEVER, executorBlockade, masterBlockade);

			PMO_RMIHelper.bind(exec, PMO_Consts.EXECUTOR_SERVICE_NAMES.get(i));

			executors.add(exec);
			execNames[i] = PMO_Consts.EXECUTOR_SERVICE_NAMES.get(i);
		}

		// przygotowac zadania
		PMO_TaskSendersTeam team = new PMO_TaskSendersTeam(dispatcher,
				PMO_Consts.NORMAL_WORK_MULTIPLE_SENDERS_PER_EXECUTOR, PMO_Consts.NORMAL_WORK_MULTIPLE_TASKS_PER_SENDER,
				execNames);

		// uruchomic transfer zadan
		team.start();

		// odczekać na dostarczenie zadań
		PMO_TimeHelper.sleep(2000);

		// odblokować pracę serwisów
		executorBlockade.trigger(true);

		PMO_TimeHelper.sleep(requredTimeEstimation);

		executorBlockade.enable(true);
		receiverBlockade.trigger(true);

		PMO_TimeHelper.sleep(requredTimeEstimation);

		receiverBlockade.enable(true);
		masterBlockade.enable(true);

		Collection<Long> tasksSended = team.getTaskIDs();
		Collection<Long> tasksExecuted = new ArrayList<Long>();
		Collection<Long> tasksReceived = receiver.getTaskIDs();

		executors.forEach(e -> tasksExecuted.addAll(e.getTaskIDs()));

		assertFalse(repetitionsDetected(tasksExecuted), "Wykryto powtórzenia identyfikatorów przetwarzanych zadań");
		assertFalse(repetitionsDetected(tasksReceived), "Wykryto powtórzenia identyfikatorów dostarczonych wyników");
		assertTrue(testEquivalence(tasksSended, tasksExecuted),
				"Wykryto rozbieżność pomiędzy zadaniami wysłanymi a przetworzonymi");
		assertTrue(testEquivalence(tasksSended, tasksReceived),
				"Wykryto rozbieżność pomiędzy zadaniami wysłanymi a odebranymi wynikami");

	}

	private void insertTests(boolean priority) {
		// utworzyc wlasne serwisy

		// ES może działać wolniej od receivera, bo przetwarza wiele zadań jednocześnie
		long executorServiceSlowdown = PMO_Consts.NORMAL_WORK_RECEIVER_SLOWDOWN * PMO_Consts.CONCURRENT_INSERT_EXECUTORS
				* PMO_Consts.CONCURRENT_INSERT_EXECUTORS;

		long numberOfTasks = PMO_Consts.CONCURRENT_INSERT_TASKS_PER_SENDER
				* PMO_Consts.CONCURRENT_INSERT_SENDERS_PER_EXECUTOR * PMO_Consts.CONCURRENT_INSERT_EXECUTORS;
		long requredTimeEstimation = numberOfTasks * PMO_Consts.NORMAL_WORK_RECEIVER_SLOWDOWN + executorServiceSlowdown;
		requredTimeEstimation = (long) (requredTimeEstimation * PMO_Consts.NORMAL_WORK_TIMECORRECTION);

		log("Number of tasks           = " + numberOfTasks);
		log("Time required             = " + requredTimeEstimation);
		log("Executor service slowdown = " + executorServiceSlowdown);

		PMO_Barrier masterBlockade = new PMO_Barrier(PMO_Consts.CONCURRENT_INSERT_EXECUTORS + 1, false, true, true,
				"master");
		PMO_Barrier executorBlockade = new PMO_Barrier(PMO_Consts.CONCURRENT_INSERT_EXECUTORS, true, true, true,
				"executor");

		PMO_Receiver receiver = PMO_Receiver.create(PMO_Consts.NORMAL_WORK_RECEIVER_SLOWDOWN, PMO_Consts.BLOCK_NEVER,
				null, masterBlockade);

		PMO_RMIHelper.bind(receiver, PMO_Consts.RECEIVER_SERVICE_NAME);

		// przekazac info o nazwie receivera
		try {
			dispatcher.setReceiverServiceName(PMO_Consts.RECEIVER_SERVICE_NAME);
		} catch (RemoteException e) {
			e.printStackTrace();
			fail("W trakcie metody setReceiverServiceName doszlo do wyjatku " + e.toString());
		}

		List<PMO_ExecutorService> executors = new ArrayList<>();
		String execNames[] = new String[PMO_Consts.CONCURRENT_INSERT_EXECUTORS];

		for (int i = 0; i < PMO_Consts.CONCURRENT_INSERT_EXECUTORS; i++) {
			PMO_ExecutorService exec = PMO_ExecutorService.create(PMO_Consts.CONCURRENT_INSERT_TASKS_ALLOWED,
					executorServiceSlowdown, PMO_Consts.BLOCK_NEVER, executorBlockade, masterBlockade);

			PMO_RMIHelper.bind(exec, PMO_Consts.EXECUTOR_SERVICE_NAMES.get(i));

			executors.add(exec);
			execNames[i] = PMO_Consts.EXECUTOR_SERVICE_NAMES.get(i);
		}

		// przygotowac zadania
		PMO_TaskSendersTeam team = new PMO_TaskSendersTeam(dispatcher,
				PMO_Consts.CONCURRENT_INSERT_SENDERS_PER_EXECUTOR, PMO_Consts.CONCURRENT_INSERT_TASKS_PER_SENDER,
				execNames);

		// uruchomic transfer zadan
		team.start();

		// odczekać na dostarczenie zadań
		PMO_TimeHelper.sleep(2000);

		// przygotować dostarczanie nowych zadań
		for (int i = 0; i < executors.size(); i++) {
			executors.get(i).taskSubmissionTestPrepare(dispatcher, execNames[i], priority);
		}

		// odblokować pracę serwisów
		executorBlockade.trigger(true);

		PMO_TimeHelper.sleep(requredTimeEstimation);

		masterBlockade.enable(true);

		// sprawdzic poprawnosc pracy

		executors.forEach(e -> e.test());
	}

//	@Disabled
	@RepeatedTest(PMO_Consts.TEST_REPETITIONS)
	public void concurrentTaskInsertTest() {
		log("concurrentTaskInsertTest");
		insertTests(false);
	}

//	@Disabled
	@RepeatedTest(PMO_Consts.TEST_REPETITIONS)
	public void priorityTest() {
		log("concurrentTaskInsertTest");
		insertTests(true);
	}
}
