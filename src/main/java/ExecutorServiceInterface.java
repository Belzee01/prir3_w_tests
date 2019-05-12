import java.rmi.Remote;
import java.rmi.RemoteException;;

/**
 * Interfejs zdalnego systemu realizacji zadań.
 */
public interface ExecutorServiceInterface extends Remote {
    /**
     * Liczba zadań, które serwis może jednocześnie realizować.
     * 
     * @return maksymalna liczba zadań, które jednocześnie może realizować dany
     *         serwis.
     * @exception RemoteException przekazywany wyjątek 
     */
    public int numberOfTasksAllowed() throws RemoteException;

    /**
     * Metoda wykonując obliczenia. Można ją wykonywać współbieżnie. Maksymalną
     * liczbę jednoczesnych użyć można poznać poprzez metodę numberOfTasksAllowed.
     * Metoda blokuje wywołujący ją wątek na czas niezbędny do wykonania zadania.
     * 
     * @param task zadanie do wykonania
     * @return rezultat pracy
     * @exception RemoteException przekazywany wyjątek 
     */
    public long execute(TaskInterface task) throws RemoteException;
}