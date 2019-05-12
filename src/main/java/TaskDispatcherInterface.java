import java.rmi.Remote;
import java.rmi.RemoteException;;

/**
 * Interfejs systemu kolejkowania i rozdziału zadań.
 */
public interface TaskDispatcherInterface extends Remote {

    /**
     * Metoda pozwala na przekazanie do systemu nazwy serwisu, do którego należy
     * odsyłać wyniki przeprowadzonych obliczeń. Serwis, do którego wysyłane są
     * wyniki obliczeń jest także serwisem RMI - stąd do jego lokalizacji wystarczy
     * znajomość nazwy, pod którą został on zarejestrowany.
     * 
     * @param name nazwa zdalnego serwisu
     * @exception RemoteException przekazywany wyjątek
     */
    public void setReceiverServiceName(String name) throws RemoteException;

    /**
     * Metoda służy do wprowadzania zadań do systemu. Metoda musi zawsze i
     * współbieżnie wprowadzać zadania do systemu. System musi pozwalać na dodawanie
     * nowych zadań nawet w przypadku, gdy inne zadania są właśnie realizowane.
     * Metoda nie może blokować pracy użytkownika na czas znacząco dłuższy od
     * potrzebnego do umieszczenia zadania w kolejce.
     * 
     * @param task                przekazywane zadanie
     * @param executorServiceName nazwa serwisu RMI, do którego należy dostarczyć
     *                            zadanie celem jego wykonania.
     * @param priority            true oznacza, że zadanie jest priorytetowe i
     *                            powinno być wykonane możliwie szybko. <br>
     *                            <br>
     *                            Niech serwis executorServiceName będzie obciążony
     *                            pracą i wykonuje maksymalną, dozwoloną liczbę
     *                            zadań i niech wszystkie zadania dla tego serwisu
     *                            są niepriorytetowe. Jeśli metoda addTask, która
     *                            wprowadza do serwisu nowe zadanie dla tego samego
     *                            executorServiceName zakończy się przed końcem
     *                            pracy metod ExecutorServiceInterface.execute to
     *                            jeśli nowe zadanie jest priorytetowe, to musi
     *                            zostać uruchomione jako zadanie następne. Jeśli
     *                            jest to zadanie zwykłe to może zostać uruchomione
     *                            w dowolnej kolejności. Jeśli w serwise są inne
     *                            zadanie priorytetowe to nowe zadanie priorytetowe
     *                            może zostać wykonane w dowolnej kolejności ale
     *                            przed zadaniami zwykłymi. <br>
     *                            false oznacza zadanie zwykłe
     * @exception RemoteException przekazywany wyjątek
     */
    public void addTask(TaskInterface task, String executorServiceName, boolean priority) throws RemoteException;

}