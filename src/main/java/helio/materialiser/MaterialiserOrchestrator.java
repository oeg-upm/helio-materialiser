package helio.materialiser;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import helio.framework.materialiser.mappings.DataSource;
import helio.framework.materialiser.mappings.HelioMaterialiserMapping;
import helio.framework.materialiser.mappings.RuleSet;
import helio.materialiser.configuration.HelioConfiguration;
import helio.materialiser.executors.AsynchronousExecutableMapping;
import helio.materialiser.executors.SynchronousExecutableMapping;

/**
 * This class manages a set of mappings, by ordering their elements according to their dependencies, and creating synchronous or asynchronous tasks to generate the RDF.
 * @author Andrea Cimmino
 *
 */
public class MaterialiserOrchestrator {

	private static Logger logger = LogManager.getLogger(MaterialiserOrchestrator.class);
	private List<SynchronousExecutableMapping> optimisedSynchronousMappings;	
	protected Timer time;
	
	
	/**
	 * This constructor receives a valid {@link HelioMaterialiserMapping} object
	 * @param mappings a valid {@link HelioMaterialiserMapping} object
	 */
	public MaterialiserOrchestrator(HelioMaterialiserMapping mappings) {
		optimisedSynchronousMappings = new CopyOnWriteArrayList<>();
		time = new Timer();
		optimisedMapping(mappings);
	}

	/**
	 * This method registers {@link AsynchronousExecutableMapping} tasks to generate RDF asynchronously.
	 * @param synchronousTask a {@link SynchronousExecutableMapping} that will be wrapped into an {@link AsynchronousExecutableMapping} and executed asynchronously
	 * @param refresh a valid time in milliseconds that specifies the periodic-time that this {@link AsynchronousExecutableMapping} will be called each time
	 */
	public void registerAsynchronousSources(SynchronousExecutableMapping synchronousTask, Integer refresh) {
		time.scheduleAtFixedRate(new AsynchronousExecutableMapping(synchronousTask), 0, refresh);
	}
	
	/**
	 * This method updates all the registered {@link SynchronousExecutableMapping} tasks
	 */
	public void updateSynchronousSources() {
		try {
		
			optimisedSynchronousMappings.parallelStream().forEach(SynchronousExecutableMapping::generateRDFSynchronously);
			HelioConfiguration.EVALUATOR.linkData();
		} catch (Exception e) {
			logger.error(e.toString());
		}
	
	}
	
	
	// -- Optmisation
	
	/**
	 * This method re-arranges the elements of a {@link HelioMaterialiserMapping} into a set of {@link OptimisedMapping} that generate the RDF more efficiently
	 * @param mappings a valid {@link HelioMaterialiserMapping} object
	 */
	public void optimisedMapping(HelioMaterialiserMapping mappings) {
		time.cancel();
		time.purge();
		time = new Timer();
		optimisedSynchronousMappings.clear();
		List<DataSource> datasources = mappings.getDatasources();
		List<RuleSet> ruleSets = mappings.getRuleSets();
		for(int index=0; index < datasources.size(); index++) {
			DataSource datasource = datasources.get(index);
			List<RuleSet> comaptibleRuleSets = new ArrayList<>();
			for(int pointer=0; pointer < ruleSets.size(); pointer++) {
				RuleSet ruleSet = ruleSets.get(pointer);
				if(ruleSet.hasDataSourceId(datasource.getId()))
					comaptibleRuleSets.add(ruleSet);
			}
			OptimisedMapping optimisedMapping = new OptimisedMapping(datasource,comaptibleRuleSets);
			SynchronousExecutableMapping synchronousTask = new SynchronousExecutableMapping(optimisedMapping.getDataSource(),optimisedMapping.getRuleSets(), mappings.getLinkRules() );
			if(datasource.getRefresh()==null) {
				optimisedSynchronousMappings.add(synchronousTask);
			}else {
				registerAsynchronousSources(synchronousTask, datasource.getRefresh());
			}
			
		}
	}

	/**
	 * This method closes the underneath processes, like the asynchronous tasks registered.
	 */
	public void close() {
		time.cancel();
		time.purge();
	}
}
