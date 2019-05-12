import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PMO_TaskSendersTeam implements PMO_Testable {

	private final Collection<PMO_TaskSender> taskSenders;
	private final Map<String, Collection<PMO_TaskSender>> taskSendersForExecutorService;
	private final PMO_Barrier initialSynchronization;

	private Thread createAndStartThread(PMO_TaskSender sender) {
		Thread th = new Thread(sender);

		th.setDaemon(true);
		th.start();

		return th;
	}

	public PMO_TaskSendersTeam(TaskDispatcherInterface dispatcher, int sendersPerExecutorService, int tasksPerSender,
			String... executorServices) {

		int senders = executorServices.length * sendersPerExecutorService;
		taskSenders = new ArrayList<>(senders);
		taskSendersForExecutorService = new HashMap<String, Collection<PMO_TaskSender>>();

		initialSynchronization = new PMO_Barrier(senders, true, true, true, "initialSenderSynchro");

		PMO_TaskSender sender;

		for (String executorServiceName : executorServices) {
			taskSendersForExecutorService.put(executorServiceName, new ArrayList<PMO_TaskSender>());
			for (int i = 0; i < sendersPerExecutorService; i++) {
				sender = new PMO_TaskSender(dispatcher, executorServiceName, tasksPerSender, initialSynchronization);
				createAndStartThread(sender);
				taskSenders.add(sender);
				taskSendersForExecutorService.get(executorServiceName).add(sender);
			}
		}
	}

	public void start() {
		initialSynchronization.trigger();
	}

	@Override
	public void test() {
		taskSenders.forEach(t -> t.test());
	}

	public Collection<Long> getTaskIDs(String executorServiceName) {
		List<Long> ids = new ArrayList<>();

		for (PMO_TaskSender sender : taskSendersForExecutorService.get(executorServiceName)) {
			ids.addAll(sender.taskIDs());
		}
		return ids;
	}

	public Collection<Long> getTaskIDs() {
		List<Long> ids = new ArrayList<>();

		taskSenders.forEach(s -> ids.addAll(s.taskIDs()));

		return ids;
	}
	
}
