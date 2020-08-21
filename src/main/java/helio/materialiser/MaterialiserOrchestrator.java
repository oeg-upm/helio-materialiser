package helio.materialiser;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.concurrent.CopyOnWriteArrayList;
import helio.framework.materialiser.mappings.DataSource;
import helio.framework.materialiser.mappings.HelioMaterialiserMapping;
import helio.framework.materialiser.mappings.RuleSet;
import helio.materialiser.executors.AsynchronousExecutableMapping;
import helio.materialiser.executors.SynchronousExecutableMapping;


public class MaterialiserOrchestrator {

	private List<SynchronousExecutableMapping> optimisedSynchronousMappings;	
	protected Timer time;

	public MaterialiserOrchestrator(HelioMaterialiserMapping mappings) {
		optimisedSynchronousMappings = new CopyOnWriteArrayList<>();
		time = new Timer();
		optimisedMapping(mappings);
	}

	public void registerAsynchronousSources(SynchronousExecutableMapping synchronousTask, Integer refresh) {
		time.schedule(new AsynchronousExecutableMapping(synchronousTask), refresh);
	}
	
	// TODO: add the same method receiving the query
	public void updateSynchronousSources() {
		try {
			optimisedSynchronousMappings.parallelStream().forEach( elem -> elem.generateRDFSynchronously());
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	
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
			SynchronousExecutableMapping synchronousTask = new SynchronousExecutableMapping(optimisedMapping.getDataSource(),optimisedMapping.getRuleSets(), mappings.getLinkRules() );
			if(datasource.getRefresh()==null) {
				optimisedSynchronousMappings.add(synchronousTask);
			}else {
				registerAsynchronousSources(synchronousTask, datasource.getRefresh());
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
