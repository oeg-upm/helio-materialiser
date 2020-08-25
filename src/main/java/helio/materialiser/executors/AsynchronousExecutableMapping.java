package helio.materialiser.executors;

import java.util.TimerTask;

import helio.materialiser.HelioMaterialiser;

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
		HelioMaterialiser.EVALUATOR.linkData();
	}
	
}
