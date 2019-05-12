import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.IntStream;

public class PMO_TaskSender implements Runnable, PMO_Testable {

	private final TaskDispatcherInterface dispatcher;
	private final PMO_Barrier initialSynchronization;
	private final ArrayList<PMO_Task> tasksToSend;
	private final String executorServiceName;
	private volatile boolean addTaskExceptionDetected;
	private volatile boolean allTasksSended;

	public PMO_TaskSender(TaskDispatcherInterface dispatcher, String executorServiceName, int howManyTasks,
			PMO_Barrier initialSynchronization) {
		this.dispatcher = dispatcher;
		this.executorServiceName = executorServiceName;
		this.initialSynchronization = initialSynchronization;
		tasksToSend = new ArrayList<PMO_Task>(howManyTasks);

		IntStream.range(0, howManyTasks).forEach(i -> tasksToSend.add(PMO_TaskGenerator.getTask()));
	}

	@Override
	public void run() {
		
		initialSynchronization.await();
		
		tasksToSend.forEach(t -> {
			try {
				dispatcher.addTask(t, executorServiceName, false);
				Thread.yield();
			} catch (RemoteException e) {
				e.printStackTrace();
				addTaskExceptionDetected = true;
			}
		});
		allTasksSended = true;
	}
	
	public Collection<Long> taskIDs() {
		Collection<Long> ids = new ArrayList<>( tasksToSend.size() );
		tasksToSend.forEach( t -> ids.add(t.taskID()));
		return ids;
	}
	
	@Override
	public void test() {
		assertFalse(addTaskExceptionDetected, "W trakcie wykonywania addTask doszło do wyjątku");
		assertTrue( allTasksSended, "Oczekiwano, że wszystkie zadania zostaną odebrane przez serwis");
	}
}
