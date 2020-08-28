package helio.materialiser.executors;

import java.util.TimerTask;

import helio.framework.materialiser.MaterialiserCache;
import helio.materialiser.configuration.HelioConfiguration;

/**
 * This class wraps {@link SynchronousExecutableMapping} in order for them to be executed asynchronously.
 * @author Andrea Cimmino
 *
 */
public class AsynchronousExecutableMapping extends TimerTask {

	private SynchronousExecutableMapping synchronousTask;
	
	/**
	 * This constructor wraps the provided {@link SynchronousExecutableMapping} into an {@link AsynchronousExecutableMapping} that will be executed asynchronously 
	 * @param synchronousTask a {@link SynchronousExecutableMapping} to be executed asynchronously
	 */
	public AsynchronousExecutableMapping(SynchronousExecutableMapping synchronousTask) {
		this.synchronousTask = synchronousTask;
	}
	
	/**
	 * This method creates the RDF from the heterogeneous sources, and then, injects the RDF in the {@link MaterialiserCache}
	 */
	public void generateRDFSynchronously() {
		synchronousTask.generateRDFSynchronously();
	}

	@Override
	public void run() {
		generateRDFSynchronously();
		HelioConfiguration.EVALUATOR.linkData();
	}
	
}
