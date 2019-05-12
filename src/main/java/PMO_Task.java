
public class PMO_Task implements TaskInterface {

	private final long taskID;
	
	public PMO_Task(long taskID) {
		this.taskID = taskID;
	}
	
	@Override
	public long taskID() {
		return taskID;
	}

}
