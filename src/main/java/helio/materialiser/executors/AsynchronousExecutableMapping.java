package helio.materialiser.executors;

import java.util.TimerTask;

public class AsynchronousExecutableMapping extends TimerTask {

	private SynchronousExecutableMapping synchronousTask;
	
	public AsynchronousExecutableMapping(SynchronousExecutableMapping synchronousTask) {
		this.synchronousTask = synchronousTask;
	}
	
	public void generateRDFSynchronously() {
		synchronousTask.generateRDFSynchronously();
	}

	@Override
	public void run() {
		generateRDFSynchronously();
	}
	
}
