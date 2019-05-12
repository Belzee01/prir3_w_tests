
public interface PMO_LogSource {
	public default void log(String txt) {
		logS(txt);
	}

	public default void error(String txt) {
		errorS(txt);
	}

	public static void logS(String txt) {
		PMO_Log.log(txt);
	}

	public static void errorS(String txt) {
		logS(txt);
	}

}
