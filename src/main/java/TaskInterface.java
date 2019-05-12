import java.io.Serializable;

/**
 * Interfejs reprezentujący zadanie obliczeniowe.
 */
public interface TaskInterface extends Serializable {
    /**
     * Metoda zwraca unikalny identyfikator zadania.
     * 
     * @return unikalny identyfikator zadania.
     */
    public long taskID();
}
