package helio.materialiser.executors;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.TimerTask;
import java.util.concurrent.ForkJoinPool;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import helio.framework.materialiser.mappings.DataSource;
import helio.framework.materialiser.mappings.RuleSet;
import helio.materialiser.configuration.HelioConfiguration;

public class AsynchronousExecutableMapping extends TimerTask {

	
	private DataSource dataSource;
	private List<RuleSet> ruleSets ;
	private static Logger logger = LogManager.getLogger(SynchronousExecutableMapping.class);
	

	public AsynchronousExecutableMapping(DataSource dataSource, List<RuleSet> ruleSets) {
		this.dataSource = dataSource;
		this.ruleSets = ruleSets;
		
	}

	
	public void generateRDFSynchronously() {
		// TODO: check if it is worthwile to put here a code 
		Queue<String> dataFragments = dataSource.getDataHandler().splitData(dataSource.getDataProvider().getData());
		System.out.println("Initial: "+dataFragments.size());
		/*
		List<ExecutableRecursiveRule> subTasks = new ArrayList<>();
		for(int index=0; index < ruleSets.size(); index ++) {
			RuleSet rs = ruleSets.get(index);
			ExecutableRecursiveRule exRule = new ExecutableRecursiveRule(rs, dataSource, new ArrayList<String>(dataFragments));
			subTasks.add(exRule);
		}
		ForkJoinTask.invokeAll(subTasks);
		*/
		 String dataFragment = dataFragments.poll();
		 ForkJoinPool commonPool =  new ForkJoinPool(HelioConfiguration.THREADS_INJECTING_DATA);
		  while(dataFragment!=null) {
			commonPool.invokeAll(getSubTasks(dataFragment));
			// go for next fragment
			dataFragment = dataFragments.poll();
		}
		  try {
			  commonPool.shutdown();
			  commonPool.awaitTermination(HelioConfiguration.SYNCHRONOUS_TIMEOUT, HelioConfiguration.SYNCHRONOUS_TIMEOUT_TIME_UNIT);
			  commonPool.shutdownNow();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("-- end --");
	}


	
	public List<ExecutableRule> getSubTasks(String dataFragment){
		List<ExecutableRule> subTasks = new ArrayList<>();
		for(int index=0; index < ruleSets.size(); index ++) {
			try {
				RuleSet rs = ruleSets.get(index);
				ExecutableRule exRule = new ExecutableRule(rs, dataSource, dataFragment);
				subTasks.add(exRule);
			}catch(Exception e) {
				logger.error(e.getMessage());
			}
		}
		return subTasks;
	}





	@Override
	public void run() {
		generateRDFSynchronously();
	}
	
}
