package helio.materialiser.executors;

import java.util.List;
import java.util.concurrent.Callable;
import helio.framework.materialiser.mappings.DataSource;
import helio.framework.materialiser.mappings.LinkRule;
import helio.framework.materialiser.mappings.RuleSet;

/**
 * This class implements a synchronous task that generates RDF
 * @author Andrea Cimmino
 *
 */
public class SynchronousExecutableMapping extends AbstractExecutableMapping implements Callable<Void>{

	/**
	 * This constructor initializes a {@link SynchronousExecutableMapping} with a {@link DataSource} and a {@link Set} of {@link RuleSet}
	 * @param dataSource a valid {@link DataSource}
	 * @param ruleSets a valid {@link Set} of {@link RuleSet}
	 */
	public SynchronousExecutableMapping(DataSource dataSource, List<RuleSet> ruleSets) {
		super(dataSource, ruleSets);
	}
	
	/**
	 * This constructor initializes a {@link SynchronousExecutableMapping} with a {@link DataSource}, a {@link Set} of {@link RuleSet}, and a {@link Set} of {@link LinkRule}
	 * @param dataSource a valid {@link DataSource}
	 * @param ruleSets a valid {@link Set} of {@link RuleSet}
	 * @param linkingRules a valid {@link Set} of {@link LinkRule}
	 */
	public SynchronousExecutableMapping(DataSource dataSource, List<RuleSet> ruleSets, List<LinkRule> linkingRules) {
		super(dataSource, ruleSets, linkingRules);
	}

	@Override
	public Void call() throws Exception {
		generateRDFSynchronously();
		return null;
	}

	
}
