package helio.materialiser.executors;

import java.util.List;
import java.util.concurrent.Callable;
import helio.framework.materialiser.mappings.DataSource;
import helio.framework.materialiser.mappings.LinkRule;
import helio.framework.materialiser.mappings.RuleSet;

public class SynchronousExecutableMapping extends AbstractExecutableMapping implements Callable<Void>{

	
	public SynchronousExecutableMapping(DataSource dataSource, List<RuleSet> ruleSets) {
		super(dataSource, ruleSets);
	}
	
	public SynchronousExecutableMapping(DataSource dataSource, List<RuleSet> ruleSets, List<LinkRule> linkingRules) {
		super(dataSource, ruleSets, linkingRules);
	}



	@Override
	public Void call() throws Exception {
		generateRDFSynchronously();
		return null;
	}

	
}
