package helio.materialiser.executors;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.TimerTask;
import java.util.concurrent.ForkJoinPool;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import helio.framework.materialiser.Evaluator;
import helio.framework.materialiser.mappings.DataSource;
import helio.framework.materialiser.mappings.EvaluableExpression;
import helio.framework.materialiser.mappings.Rule;
import helio.framework.materialiser.mappings.RuleSet;
import helio.materialiser.HelioMaterialiser;
import helio.materialiser.configuration.HelioConfiguration;
import helio.materialiser.evaluator.H2Evaluator;
import helio.materialiser.exceptions.MalformedUriException;

public class AsynchronousExecutableMapping extends TimerTask {

	
	private DataSource dataSource;
	private List<RuleSet> ruleSets ;
	
	private Evaluator evaluator = new H2Evaluator();
	private static Logger logger = LogManager.getLogger(AsynchronousExecutableMapping.class);
	public static ForkJoinPool forkJoinPool = new ForkJoinPool(1);


	public AsynchronousExecutableMapping(DataSource dataSource, List<RuleSet> ruleSets) {
		this.dataSource = dataSource;
		this.ruleSets = ruleSets;
	}

	@Override
	public void run() {
		generateRDFSynchronously();
	}
	
	public void generateRDFSynchronously() {
		// TODO: check if it is worthwile to put here a code 
		Queue<String> dataFragments = dataSource.getDataHandler().splitData(dataSource.getDataProvider().getData());
		String dataFragment = dataFragments.poll();
		while(dataFragment!=null) {
			for(int index=0; index < ruleSets.size(); index ++) {
				try {
					RuleSet rs = ruleSets.get(index);
					throwTranslationThread(rs, dataFragment);
				}catch(Exception e) {
					logger.error(e.getMessage());
				}
			}
			
			// go for next fragment
			dataFragment = dataFragments.poll();
		}
	}

	private void throwTranslationThread(RuleSet ruleSet, String dataFragment) throws MalformedUriException, InterruptedException {
		ForkJoinPool pool = ForkJoinPool.commonPool();
		String subject = instantiateExpression(ruleSet.getSubjectTemplate(), dataFragment);
		if(subject!=null) {
			HelioMaterialiser.HELIO_CACHE.deleteGraph(createGraphIdentifier(subject));
			for(int index= 0; index < ruleSet.getProperties().size();index++) {
				Rule rule = ruleSet.getProperties().get(index);
				ExecutableRule exRule = new ExecutableRule(subject, rule, dataSource, dataFragment);
				pool.execute(exRule);
			}
		}else {
			throw new MalformedUriException("Subject could not be formed due to data references specified in the subject that are not present in the data");
		}
		if(HelioConfiguration.WAIT_FOR_SYNCHRONOUS_TRANSFORMATION_TO_FINISH) {
			pool.awaitTermination(HelioConfiguration.SYNCHRONOUS_TIMEOUT, HelioConfiguration.SYNCHRONOUS_TIMEOUT_TIME_UNIT);
			pool.shutdownNow();
		}
	}
	
	private String createGraphIdentifier(String subject) {
		StringBuilder builder = new StringBuilder();
		builder.append(subject).append("/").append(String.valueOf(this.dataSource.getId().hashCode()).replace("-", "0"));
		return builder.toString();
	}
	
	private String instantiateExpression(EvaluableExpression expression, String dataChunk) {
		Map<String,String> dataReferencesSolved = new HashMap<>();
		List<String> dataReferences = expression.getDataReferences();
		for(int index=0;index < dataReferences.size(); index++) {
			String reference = dataReferences.get(index);
			String dataReferenceSolved = dataSource.getDataHandler().filter(reference, dataChunk);
			if(dataReferenceSolved==null) {
				logger.warn("The reference '"+reference+"' that was provided has no data in the fetched document "+dataChunk);
			}else {
				dataReferencesSolved.put(reference, dataReferenceSolved);
			}
		}
		String instantiatedEE = expression.instantiateExpression(dataReferencesSolved);
		if(instantiatedEE!=null) {
			instantiatedEE = evaluator.evaluateExpresions(instantiatedEE);
		}
		return instantiatedEE;
	}
	
}
