import java.util.List;

public class PMO_Consts {
	public static final long BLOCKING_FOREVER = 1000000;
	public static final int BLOCK_NEVER = 100000000;

	public static final String RECEIVER_SERVICE_NAME = "ReCeIvEr";
	public static final List<String> EXECUTOR_SERVICE_NAMES = List.of("Alfa", "Beta", "Gamma", "Delta", "Epsilon", "Fi",
			"Omega", "Pi", "Ro", "Theta", "Zeta");

	public static final long NORMAL_WORK_RECEIVER_SLOWDOWN = 40;
	public static final int NORMAL_WORK_TASKS_ALLOWED = 4;
	public static final int NORMAL_WORK_SENDERS_PER_EXECUTOR = 12;
	public static final int NORMAL_WORK_TASKS_PER_SENDER = 20;
	public static final double NORMAL_WORK_TIMECORRECTION = 1.15;

	public static final int NORMAL_WORK_MULTIPLE_EXECUTORS = 5;
	public static final int NORMAL_WORK_MULTIPLE_TASKS_ALLOWED = 2;
	public static final int NORMAL_WORK_MULTIPLE_SENDERS_PER_EXECUTOR = 5;
	public static final int NORMAL_WORK_MULTIPLE_TASKS_PER_SENDER = 10;

	public static final int CONCURRENT_INSERT_EXECUTORS = 3;
	public static final int CONCURRENT_INSERT_TASKS_ALLOWED = 1;
	public static final int CONCURRENT_INSERT_SENDERS_PER_EXECUTOR = 1;
	public static final int CONCURRENT_INSERT_TASKS_PER_SENDER = 10;
	
	public static final int INDEPENDENT_TASK_EXECUTION_RECEIVER_BLOCKADE_AFTER = 5;
	
	public static final int TEST_REPETITIONS = 1;
}
