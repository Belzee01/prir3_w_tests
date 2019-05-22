import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;

public class Start extends UnicastRemoteObject implements TaskDispatcherInterface {

    public static void main(String[] args) {
        boolean connected = false;
        while (!connected) {
            try {
                Registry registry = LocateRegistry.getRegistry();

                registry.bind("TaskDispatcher", new Start());
                System.out.println("Server ready");
                connected = true;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private ReceiverInterface receiver;

    private ConcurrentHashMap<String, TaskConsumer> consumers;

    private LinkedBlockingQueue<TaskResult> resultsQueue;

    public Start() throws RemoteException {
        super();
        this.consumers = new ConcurrentHashMap<>();
        this.resultsQueue = new LinkedBlockingQueue<>();
    }

    @Override
    public void setReceiverServiceName(String name) {
        System.out.println("Setting the receiver name to: " + name);
        receiver = (ReceiverInterface) Helper.connect(name);

        new Thread(() -> {
            while (true) {
                Optional.ofNullable(this.resultsQueue.poll())
                        .ifPresent(r -> {
                            try {
                                receiver.result(r.getTaskId(), r.getResult());
                            } catch (RemoteException e) {
                                e.printStackTrace();
                            }
                        });
            }
        }).start();
    }

    @Override
    public void addTask(TaskInterface task, String executorServiceName, boolean priority) {
        synchronized (this) {
            this.consumers.putIfAbsent(executorServiceName, new TaskConsumer(executorServiceName));
            this.consumers.get(executorServiceName).add(task, priority);
        }
    }

    class TaskConsumer {
        private PriorityQueue<TaskPriority> priorityQueue;
        private ExecutorServiceInterface es;
        private ExecutorService service;

        private int threadPoolSize;

        private final Object lock;

        public TaskConsumer(String serviceName) {
            this.priorityQueue = new PriorityQueue<>((t1, t2) -> Boolean.compare(t1.isPriority(), t2.isPriority()));
            this.es = (ExecutorServiceInterface) Helper.connect(serviceName);
            try {
                this.threadPoolSize = this.es.numberOfTasksAllowed();
                this.service = Executors.newFixedThreadPool(threadPoolSize);
            } catch (RemoteException e) {
                e.printStackTrace();
            }

            this.lock = new Object();
        }

        public void add(TaskInterface task, boolean priority) {
            synchronized (this.lock) {
                priorityQueue.add(new TaskPriority(task, priority));
            }

            for (int i = 0; i < threadPoolSize; i++) {
                this.service
                        .execute(() -> {
                            while (true) {
                                Optional.ofNullable(this.priorityQueue.poll())
                                        .ifPresent(it -> {
                                            try {
                                                long execute = es.execute(it.getTask());
                                                resultsQueue.add(new TaskResult(it.getTask().taskID(), execute));
                                            } catch (RemoteException e) {
                                                e.printStackTrace();
                                                Thread.currentThread().interrupt();
                                            }
                                        });
                            }
                        });
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
