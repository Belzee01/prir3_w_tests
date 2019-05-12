import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

public class Start implements PMO_LogSource {

    public static void main(String[] args) {
        try {
            TaskDispatcherInterface obj = new Server();
            TaskDispatcherInterface stub = (TaskDispatcherInterface) UnicastRemoteObject.exportObject(obj, 0);

            Registry registry = LocateRegistry.getRegistry();

            registry.bind("TaskDispatcher", stub);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class Server implements TaskDispatcherInterface {
        private ReceiverInterface receiver;

        private Map<String, Worker> consumers;

        public Server() {
            super();
            this.consumers = new HashMap<>();
        }

        @Override
        public void setReceiverServiceName(String name) {
            System.out.println("Setting the receiver name to: " + name);
            receiver = (ReceiverInterface) Helper.connect("rmi://localhost/" + name);
        }

        @Override
        public void addTask(TaskInterface task, String executorServiceName, boolean priority) {
            synchronized (this) {
                if (!consumers.containsKey(executorServiceName)) {
                    Worker worker = null;
                    try {
                        worker = new Worker(executorServiceName, receiver);
                    } catch (RemoteException e) {
                        e.printStackTrace();
                    }
                    consumers.put(executorServiceName, worker);
                    try {
                        consumers.get(executorServiceName).add(task, priority);
                    } catch (ExecutionException | InterruptedException e) {
                        e.printStackTrace();
                    }
                } else {
                    try {
                        consumers.get(executorServiceName).add(task, priority);
                    } catch (ExecutionException | InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        public static class Worker {
            private ExecutorServiceInterface es;
            private ReceiverInterface receiver;
            private ExecutorService executorService;

            public Worker(String serviceName, ReceiverInterface receiver) throws RemoteException {
                this.receiver = receiver;
                this.es = (ExecutorServiceInterface) Helper.connect("rmi://localhost/" + serviceName);

                executorService = new ThreadPoolExecutor(
                        es.numberOfTasksAllowed(),
                        es.numberOfTasksAllowed(),
                        0L, TimeUnit.MILLISECONDS,
                        new PriorityBlockingQueue<>(
                                es.numberOfTasksAllowed(),
                                new PriorityFutureComparator()
                        )) {

                    protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable) {
                        RunnableFuture<T> newTaskFor = super.newTaskFor(callable);
                        return new PriorityFuture<T>(newTaskFor, ((Job) callable).getPriority());
                    }
                };
            }

            public void add(TaskInterface task, boolean priority) throws ExecutionException, InterruptedException {
                synchronized (this) {
                    Job job = new Job(priority, task, es);
                    Future<Long> future = executorService.submit(job);
//                    try {
//                        long result = future.get();
//                        receiver.result(task.taskID(), result);
//                    } catch (RemoteException e) {
//                        e.printStackTrace();
//                    }
                }
            }

            class PriorityFuture<T> implements RunnableFuture<T> {

                private RunnableFuture<T> src;
                private boolean priority;

                public PriorityFuture(RunnableFuture<T> other, boolean priority) {
                    this.src = other;
                    this.priority = priority;
                }

                public boolean getPriority() {
                    return priority;
                }

                public boolean cancel(boolean mayInterruptIfRunning) {
                    return src.cancel(mayInterruptIfRunning);
                }

                public boolean isCancelled() {
                    return src.isCancelled();
                }

                public boolean isDone() {
                    return src.isDone();
                }

                public T get() throws InterruptedException, ExecutionException {
                    return src.get();
                }

                public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException {
                    return src.get();
                }

                public void run() {
                    src.run();
                }
            }

            class PriorityFutureComparator implements Comparator<Runnable> {
                public int compare(Runnable o1, Runnable o2) {
                    boolean p1 = ((PriorityFuture<?>) o1).getPriority();
                    boolean p2 = ((PriorityFuture<?>) o2).getPriority();

                    return -1 * Boolean.compare(p1, p2);
                }
            }

            class Job implements Callable<Long> {
                private boolean priority;
                private ExecutorServiceInterface es;
                private TaskInterface task;

                public Job(boolean priority, TaskInterface task, ExecutorServiceInterface es) {
                    this.priority = priority;
                    this.es = es;
                    this.task = task;
                }

                public Long call() throws Exception {
                    System.out.println("Executing: " + priority);
                    return es.execute(task);
                }

                public boolean getPriority() {
                    return priority;
                }
            }
        }

    }
}
