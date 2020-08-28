package helio.materialiser.executors;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import helio.framework.materialiser.MaterialiserCache;
import helio.framework.materialiser.mappings.DataSource;
import helio.framework.materialiser.mappings.LinkRule;
import helio.framework.materialiser.mappings.RuleSet;
import helio.materialiser.configuration.HelioConfiguration;

/**
 * This class implements the methods required to generate RDF either following a synchronous or an asynchronous approach
 * @author Andrea Cimmino
 *
 */
public abstract class AbstractExecutableMapping {

	private DataSource dataSource;
	private List<RuleSet> ruleSets ;
	private static Logger logger = LogManager.getLogger(AbstractExecutableMapping.class);
	private List<LinkRule> linkingRules;


	/**
	 * Instantiates the  {@link AbstractExecutableMapping} with a {@link DataSource}, a set of {@link RuleSet} that use the data of the provided data source. In this case no {@link LinkRule} exist related to the data of the {@link DataSource} and the {@link RuleSet}.
	 * @param dataSource a valid {@link DataSource}
	 * @param ruleSets a {@link Set} of {@link RuleSet}
	 */
	public AbstractExecutableMapping(DataSource dataSource, List<RuleSet> ruleSets) {
		this.dataSource = dataSource;
		this.ruleSets = ruleSets;
		this.linkingRules = new ArrayList<>();
	}
	
	/**
	 * Instantiates the  {@link AbstractExecutableMapping} with a {@link DataSource}, a set of {@link RuleSet} that use the data of the provided data source, and finally, with a set of {@link LinkRule}.
	 * @param dataSource a valid {@link DataSource}
	 * @param ruleSets a {@link Set} of {@link RuleSet}
	 * @param linkingRules a {@link Set} of {@link LinkRule}
	 */
	public AbstractExecutableMapping(DataSource dataSource, List<RuleSet> ruleSets, List<LinkRule> linkingRules) {
		this.dataSource = dataSource;
		this.ruleSets = ruleSets;
		this.linkingRules = linkingRules;
	}

	/**
	 * This method creates the RDF from the heterogeneous sources, and then, injects the RDF in the {@link MaterialiserCache}
	 */
	public void generateRDFSynchronously() {
		// TODO: check if it is worthwile to put here a code 
		Queue<String> dataFragments = dataSource.getDataHandler().splitData(dataSource.getDataProvider().getData());
		//p1
		/*List<ExecutableRecursiveRule> subTasks = new ArrayList<>();
		for(int index=0; index < ruleSets.size(); index ++) {
			RuleSet rs = ruleSets.get(index);
			ExecutableRecursiveRule exRule = new ExecutableRecursiveRule(rs, dataSource, new ArrayList<String>(dataFragments));
			subTasks.add(exRule);
		}
		ForkJoinTask.invokeAll(subTasks);*/
		
		//p2
		 String dataFragment = dataFragments.poll();
		 ForkJoinPool commonPool =  new ForkJoinPool(HelioConfiguration.THREADS_HANDLING_DATA);
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
		//p3
		/*String dataFragment = dataFragments.poll();
		ExecutorService executor = Executors.newFixedThreadPool(HelioConfiguration.THREADS_HANDLING_DATA);
		 while(dataFragment!=null) {
			 List<ExecutableRule> rules = getSubTasks(dataFragment);
			 try {
				executor.invokeAll(rules);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			 // go for next fragment
				dataFragment = dataFragments.poll();
			}
		 executor.shutdown();
	     try {
			executor.awaitTermination(HelioConfiguration.SYNCHRONOUS_TIMEOUT, HelioConfiguration.SYNCHRONOUS_TIMEOUT_TIME_UNIT);
			executor.shutdownNow();
	     } catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		*/
	}


	
	private List<ExecutableRule> getSubTasks(String dataFragment){
		List<ExecutableRule> subTasks = new ArrayList<>();
		for(int index=0; index < ruleSets.size(); index ++) {
			try {
				RuleSet rs = ruleSets.get(index);
				ExecutableRule exRule = new ExecutableRule(rs, dataSource, dataFragment, this.linkingRules);
				subTasks.add(exRule);
			}catch(Exception e) {
				logger.error(e.getMessage());
			}
		}
		return subTasks;
	}
}
