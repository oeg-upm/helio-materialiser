package helio.materialiser;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Timer;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ForkJoinPool;

import helio.framework.materialiser.mappings.DataSource;
import helio.framework.materialiser.mappings.HelioMaterialiserMapping;
import helio.framework.materialiser.mappings.RuleSet;
import helio.materialiser.configuration.HelioConfiguration;
import helio.materialiser.executors.AsynchronousExecutableMapping;
import helio.materialiser.executors.SynchronousExecutableMapping;


public class MaterialiserOrchestrator {

	private List<SynchronousExecutableMapping> optimisedSynchronousMappings;	
	protected Timer time;
	private ForkJoinPool pool = ForkJoinPool.commonPool();
	
	public MaterialiserOrchestrator(HelioMaterialiserMapping mappings) {
		optimisedSynchronousMappings = new CopyOnWriteArrayList<>();
		time = new Timer();
		optimisedMapping(mappings);
	}

	public void registerAsynchronousSources(OptimisedMapping optimisedMapping) {
		time.schedule(new AsynchronousExecutableMapping(optimisedMapping.getDataSource(), optimisedMapping.getRuleSets()), new Date(), optimisedMapping.getDataSource().getRefresh());
	}
	
	// TODO: add the same method receiving the query
	public void updateSynchronousSources() {
		try {
			
			optimisedSynchronousMappings.parallelStream().forEach(syncTask -> pool.execute(syncTask));
			if(HelioConfiguration.WAIT_FOR_SYNCHRONOUS_TRANSFORMATION_TO_FINISH)
				pool.awaitTermination(HelioConfiguration.SYNCHRONOUS_TIMEOUT, HelioConfiguration.SYNCHRONOUS_TIMEOUT_TIME_UNIT);
			pool.shutdownNow();
		} catch (Exception e) {
			e.printStackTrace();
		}
	
	}
	
	public boolean isSynchronousUpdateFinished() {
		return pool.isShutdown();
	}
	
	
	// -- Optmisation
	
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
			if(datasource.getRefresh()==null) {
				optimisedSynchronousMappings.add(new SynchronousExecutableMapping(optimisedMapping.getDataSource(), optimisedMapping.getRuleSets()));
			}else {
				registerAsynchronousSources(optimisedMapping);
			}
			
		}
		// TODO: MAYBE REMOVE USED RS AND DS?
		// TODO: MAYBE INFORM IF A RS HAS A POINTER TO AN INEXISTING DS OR A DS WAS NOT USED BY ANY RS?
	}


	public void close() {
		time.cancel();
		time.purge();
	}
}
