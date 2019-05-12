import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class PMO_RMIHelper {

	public static Registry getRegistry() {
		try {
			return LocateRegistry.getRegistry();
		} catch (RemoteException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static boolean bind(Remote service, String serviceName) {
		try {
			getRegistry().bind(serviceName, service);
		} catch (RemoteException | AlreadyBoundException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	public static boolean unbind(String serviceName) {
		try {
			getRegistry().unbind(serviceName);
		} catch (RemoteException | NotBoundException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	public static Remote connect(String url) {
		try {
			return java.rmi.Naming.lookup(url);
		} catch (Exception e) {
			System.err.println("W trakcie pracy metody connect doszlo do wyjatku " + e.toString());
			return null;
		}
	}
}
