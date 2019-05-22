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
import java.util.concurrent.PriorityBlockingQueue;
import java.util.stream.Stream;

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
                                receiver = (ReceiverInterface) Helper.connect(name);
                                System.out.println("Sending result: " + r.getResult() + " for task: " + r.getTaskId());
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
            this.priorityQueue = new PriorityBlockingQueue<>(10, (t1, t2) -> -1* Boolean.compare(t1.isPriority(), t2.isPriority()));
            this.es = (ExecutorServiceInterface) Helper.connect(serviceName);
            try {
                this.threadPoolSize = this.es.numberOfTasksAllowed();
                this.threadPool = new Thread[this.threadPoolSize];
            } catch (RemoteException e) {
                e.printStackTrace();
            }

            this.lock = new Object();

            for (int i = 0; i < threadPoolSize; i++) {
                this.threadPool[i] = new Thread(() -> {
                    while (true) {
                        Optional.ofNullable(this.priorityQueue.poll())
                                .ifPresent(it -> {
                                    try {
                                        ExecutorServiceInterface exec = (ExecutorServiceInterface) Helper.connect(serviceName);
                                        if (exec != null) {
                                            System.out.println("Started execution of task: " + it.getTask().taskID() + " with exec: " + serviceName);
                                            long execute = exec.execute(it.getTask());
                                            System.out.println("Executed task: " + it.getTask().taskID() + " with exec: " + serviceName);
                                            resultsQueue.add(new TaskResult(it.getTask().taskID(), execute));
                                        }
                                    } catch (RemoteException e) {
                                        System.out.println("Failed with task:"+ it.getTask().taskID()+ " and service " + serviceName);
                                        e.printStackTrace();
                                        Thread.currentThread().interrupt();
                                    }
                                });
                    }
                });
            }

            Stream.of(this.threadPool).forEach(Thread::start);
        }

        public void add(TaskInterface task, boolean priority) {
//            synchronized (this.lock) {
            priorityQueue.add(new TaskPriority(task, priority));
//            }
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
