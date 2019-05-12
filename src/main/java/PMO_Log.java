import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;

public class PMO_Log {
	private static Collection<String> log = new ConcurrentLinkedQueue<>();

	private static void add(String txt) {
		System.err.println(txt);
		log.add(txt);
	}

	public static void log(String txt) {
		StringBuilder sb = new StringBuilder();
		sb.append("[");
		sb.append(PMO_TimeHelper.timeFromStart());
		sb.append(":");
		sb.append(Thread.currentThread().getName());
		sb.append("> ");
		sb.append(txt);
		sb.append("]");
		add(sb.toString());
	}

	public static void logFormatted(String txt) {
		add(txt);
	}

	public static void showLog() {
		log.forEach(System.out::println);
	}
}
