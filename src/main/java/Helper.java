import java.rmi.Remote;

public class Helper {
	/**
	 * Metoda zwraca referencje do serwisu RMI o podanej nazwie.
	 * 
	 * @param url nazwa serwisu RMI
	 * @return referencja do namiastki reprezentujacej zdalny serwis RMI
	 */
	public static Remote connect(String url) {
		try {
			return java.rmi.Naming.lookup(url);
		} catch (Exception e) {
			System.err.println("W trakcie pracy metody connect doszlo do wyjatku");
			return null;
		}
	}
}
