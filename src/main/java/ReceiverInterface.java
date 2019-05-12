import java.rmi.Remote;
import java.rmi.RemoteException;;

/**
 * Interfejs systemu odbioru wyników obliczeń.
 */
public interface ReceiverInterface extends Remote {
    /**
     * Metoda, do której nadsyłany jest wynik obliczeń. Wyniki obliczeń mogą być
     * nadsyłane wyłącznie sekwencyjnie. Współbieżne użycie metody spowoduje błąd.
     * 
     * @param taskID numer zadania - można go poznać za pomocą metody
     *               TaskInterface.taskID()
     * @param result wynik obliczeń wykonanych za pomocą metody
     *               ExecutorServiceInterface.execute()
     * @exception RemoteException przekazywany wyjątek
     */
    public void result(long taskID, long result) throws RemoteException;
}