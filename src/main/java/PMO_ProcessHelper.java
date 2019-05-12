import java.io.IOException;
import java.util.Optional;
import java.util.stream.Stream;

public class PMO_ProcessHelper {
	/**
	 * Metoda zwraca obiekt klasy Process reprezentujacy process utworzony poprzez
	 * wykonanie command.
	 * 
	 * @return utworzony proces
	 */
	public static Process create(String ... command) throws IOException {
		ProcessBuilder pb = new ProcessBuilder(command);
		return pb.start();
	}

	/**
	 * Metoda próbuje zakończyc proces.
	 * 
	 * @param process proces do zakończenia
	 * @return czy proces wciąż żyje
	 */
	public static boolean kill(Process process) {
		process.destroy();
		PMO_TimeHelper.sleep(250);
		if (process.isAlive()) {
			process.destroyForcibly();
			PMO_TimeHelper.sleep(250);
			if ( process.isAlive() ) {
				try {
					create( "kill", Integer.toString( (int) (process.pid( ) ) ) );
					PMO_TimeHelper.sleep(250);
					if ( process.isAlive() ) {
						create( "kill", "-9", Integer.toString( (int) (process.pid( ) ) ) );
						PMO_TimeHelper.sleep(250);
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return process.isAlive();
	}

	public static String processHandle2String(ProcessHandle process) {
		ProcessHandle.Info info = process.info();
		Optional<String> commandLine = info.commandLine();
		return "PID " + process.pid() + " " + commandLine.orElse("-unknown-");
	}

	public static String childrenInfo() {
		StringBuffer sb = new StringBuffer();

		Stream<ProcessHandle> children = ProcessHandle.current().children();

		children.forEach(c -> {
			sb.append(processHandle2String(c));
			sb.append("\n");
		});

		return sb.toString();
	}

	public static void main(String[] args) throws IOException {
		Process ps = create("rmiregistry");
		System.out.println( childrenInfo() );
		kill(ps);
		System.out.println( childrenInfo() );
	}
}
