import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.stream.Stream;

public class Start extends UnicastRemoteObject implements TaskDispatcherInterface {

    public static void main(String[] args) {
        try {
            Naming.rebind("TaskDispatcher", new Start());
            System.out.println("Server ready");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private volatile ReceiverInterface receiver;

    private ConcurrentHashMap<String, TaskConsumer> consumers;

    private LinkedBlockingQueue<TaskResult> resultsQueue;

    public Start() throws RemoteException {
        super();
        this.consumers = new ConcurrentHashMap<>();
        this.resultsQueue = new LinkedBlockingQueue<>(250);
    }

    @Override
    public void setReceiverServiceName(String name) {
        System.out.println("Setting the receiver name to: " + name);
        receiver = (ReceiverInterface) Helper.connect(name);

        new Thread(() -> {
            while (true) {
                synchronized (this) {
                    Optional.ofNullable(this.resultsQueue.poll())
                            .ifPresent(r -> {
                                if (receiver != null) {
                                    try {
                                        receiver.result(r.getTaskId(), r.getResult());
                                    } catch (RemoteException e) {
                                        e.printStackTrace();
                                    }
                                }
                            });
                }
            }
        }).start();
    }

    @Override
    public void addTask(TaskInterface task, String executorServiceName, boolean priority) {
        this.consumers.putIfAbsent(executorServiceName, new TaskConsumer(executorServiceName));
        this.consumers.get(executorServiceName).add(task, priority);
    }

    class TaskConsumer {
        private PriorityBlockingQueue<TaskPriority> priorityQueue;
        private ExecutorServiceInterface es;

        private int threadPoolSize;

        private final Object lock;

        private Thread[] threadPool;

        public TaskConsumer(String serviceName) {
            this.priorityQueue = new PriorityBlockingQueue<>(250, (t1, t2) -> -1 * Boolean.compare(t1.isPriority(), t2.isPriority()));
            this.es = (ExecutorServiceInterface) Helper.connect(serviceName);
            try {
                this.threadPoolSize = this.es.numberOfTasksAllowed();
                this.threadPool = new Thread[this.threadPoolSize];
            } catch (RemoteException e) {
                e.printStackTrace();
            }
            this.lock = new Object();

            Stream.of(threadPool).forEach(t -> {
                t = new Thread(() -> {
                    while (true) {
                        try {
                            TaskPriority task = this.priorityQueue.take();
                            ExecutorServiceInterface exec = (ExecutorServiceInterface) Helper.connect(serviceName);
                            if (exec != null) {
                                long execute = exec.execute(task.getTask());
                                resultsQueue.add(new TaskResult(task.getTask().taskID(), execute));
                            }
                        } catch (RemoteException | InterruptedException e) {
                            e.printStackTrace();
                            Thread.currentThread().interrupt();
                        }
                    }
                });
                t.start();
            });
        }

        public void add(TaskInterface task, boolean priority) {
            synchronized (this.lock) {
                priorityQueue.add(new TaskPriority(task, priority));
            }
        }
    }

    class TaskPriority {
        private TaskInterface task;
        private boolean priority;

        public TaskPriority(TaskInterface task, boolean priority) {
            this.task = task;
            this.priority = priority;
        }

        public TaskInterface getTask() {
            return task;
        }

        public boolean isPriority() {
            return priority;
        }
    }

    class TaskResult {
        private long taskId;
        private long result;

        public TaskResult(long taskId, long result) {
            this.taskId = taskId;
            this.result = result;
        }

        public long getTaskId() {
            return taskId;
        }

        public long getResult() {
            return result;
        }
    }

}
