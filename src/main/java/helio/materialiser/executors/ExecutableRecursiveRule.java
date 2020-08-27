package helio.materialiser.executors;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RecursiveAction;
import org.apache.jena.datatypes.BaseDatatype;
import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


import helio.framework.materialiser.mappings.DataSource;
import helio.framework.materialiser.mappings.EvaluableExpression;
import helio.framework.materialiser.mappings.Rule;
import helio.framework.materialiser.mappings.RuleSet;
import helio.materialiser.HelioMaterialiser;
import helio.materialiser.HelioUtils;
import helio.materialiser.configuration.HelioConfiguration;
import helio.materialiser.data.handlers.RDFHandler;
import helio.materialiser.exceptions.MalformedUriException;


public class ExecutableRecursiveRule extends  RecursiveAction {

	private static final long serialVersionUID = 1L;
	private RuleSet ruleSet;
	private List<String> dataFragments;

	//private String id;
	private DataSource dataSource;
	private static Logger logger = LogManager.getLogger(ExecutableRecursiveRule.class);

	
	public ExecutableRecursiveRule(RuleSet ruleSet, DataSource dataSource, List<String> dataFragments) {
		this.dataSource = dataSource;
		this.ruleSet = ruleSet;
		//this.id = dataSource.getId();
		this.dataFragments = dataFragments;
	}

	

	
	@Override
	protected void compute() {
		try {
			if(dataFragments.size()==1) {
				if(dataSource.getDataHandler() instanceof RDFHandler && ruleSet.getProperties().isEmpty()) {
					insertRDFdata();
				}else {
					throwTranslationThread(dataFragments.get(0));
				}
			}else {
				List<String> list1 = dataFragments.subList(0, dataFragments.size()/2);
				List<String> list2 = dataFragments.subList(dataFragments.size()/2, dataFragments.size());
				if(list1.isEmpty() && list2.isEmpty()) {
					//System.out.println("done");
				}else if (!list1.isEmpty() && list2.isEmpty()) {
					invokeAll(new ExecutableRecursiveRule(ruleSet, dataSource, list2));
				}else if (list1.isEmpty() && !list2.isEmpty()) {
					invokeAll(new ExecutableRecursiveRule(ruleSet, dataSource, list1));
				}else {
					invokeAll(new ExecutableRecursiveRule(ruleSet, dataSource, list1), new ExecutableRecursiveRule(ruleSet, dataSource, list2));
				}
			}
			
		}catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	/**
	 * Inserts RDF data into the repository
	 */
	private void insertRDFdata() {
		try {
			String dataChunk = dataFragments.get(0);
			String hash = String.valueOf(dataChunk.hashCode());
			InputStream inputStream = new ByteArrayInputStream(dataChunk.getBytes(Charset.forName("UTF-8")));
			Model model = ModelFactory.createDefaultModel();
			model.read(inputStream, HelioConfiguration.DEFAULT_BASE_URI, HelioConfiguration.DEFAULT_RDF_FORMAT);
			String namedGraph = HelioUtils.createGraphIdentifier(HelioConfiguration.DEFAULT_BASE_URI, hash);
			HelioMaterialiser.HELIO_CACHE.deleteGraph(namedGraph);
			HelioMaterialiser.HELIO_CACHE.addGraph(namedGraph, model);
			
		}catch (Exception e) {
			logger.error(e.toString());
		}
	}
	
	

	private void throwTranslationThread(String dataFragment) throws MalformedUriException, InterruptedException {
		
		String subject = instantiateExpression(ruleSet.getSubjectTemplate(), dataFragment);
		ExecutorService executor = Executors.newFixedThreadPool(HelioConfiguration.THREADS_INJECTING_DATA);
		if(subject!=null) {
			HelioMaterialiser.HELIO_CACHE.deleteGraph(HelioUtils.createGraphIdentifier(subject, this.dataSource.getId()));
			for(int index= 0; index < ruleSet.getProperties().size();index++) {
				Rule rule = ruleSet.getProperties().get(index);
				executor.submit( new Runnable() {
				    @Override
				    public void run() {
				    		generateRDF(subject, rule,dataFragment);
				    }
				});
			}
		}else {
			throw new MalformedUriException("Subject could not be formed due to data references specified in the subject that are not present in the data");
		}
		 executor.shutdown();
	     executor.awaitTermination(HelioConfiguration.SYNCHRONOUS_TIMEOUT, HelioConfiguration.SYNCHRONOUS_TIMEOUT_TIME_UNIT);
	     executor.shutdownNow();
		
	}
	
	public void generateRDF(String subject, Rule rule, String dataFragment) {
		String instantiatedPredicate = instantiateExpression(rule.getPredicate(),  dataFragment);
		String instantiatedObject = instantiateExpression(rule.getObject(),  dataFragment);
		if(HelioUtils.isValidURL(subject)) {
			if(HelioUtils.isValidURL(instantiatedPredicate)) {
				if( rule.getIsLiteral() || (HelioUtils.isValidURL(instantiatedObject) && !rule.getIsLiteral())) {
					persistRDF(rule, subject, instantiatedPredicate, instantiatedObject);
				}else {
					logger.error("Genrated object has syntax errors: "+instantiatedObject);
				}
			}else {
				logger.error("Genrated predicate has syntax errors: "+instantiatedPredicate);
			}
		}else {
			logger.error("Genrated subject has syntax errors: "+subject);
		}
	}
	
	
	private void persistRDF(Rule rule, String subject, String instantiatedPredicate, String instantiatedObject) {
		Model model = ModelFactory.createDefaultModel();
		RDFNode node = createObjectJenaNode(instantiatedObject, rule);
		model.createResource(subject).addProperty(ResourceFactory.createProperty(instantiatedPredicate), node);
		HelioMaterialiser.HELIO_CACHE.addGraph(HelioUtils.createGraphIdentifier(subject, this.dataSource.getId()), model);
	}
	
     private RDFNode createObjectJenaNode(String instantiatedObject, Rule rule) {
    	 	RDFNode node = null;
 		if(rule.getIsLiteral()) {
 			if(rule.getDataType()!=null) {
 				RDFDatatype dtBuilder = new BaseDatatype(rule.getDataType());
 				node = ResourceFactory.createTypedLiteral(instantiatedObject, dtBuilder);
 			}else if(rule.getLanguage()!=null) {
 				node = ResourceFactory.createLangLiteral(instantiatedObject, rule.getLanguage());
 			}else {
 				node = ResourceFactory.createPlainLiteral(instantiatedObject);
 			}
 		}else {
 			node = ResourceFactory.createResource(instantiatedObject);
 		}
 		return node;
     }
	

	
	private String instantiateExpression(EvaluableExpression expression, String dataChunk) {
		Map<String,String> dataReferencesSolved = new HashMap<>();
		List<String> dataReferences = expression.getDataReferences();
		for(int index=0;index < dataReferences.size(); index++) {
			String reference = dataReferences.get(index);
			List<String> setFilteredData =  dataSource.getDataHandler().filter(reference, dataChunk);
			for(int counter=0; counter<setFilteredData.size(); counter++) {
				String filteredData = setFilteredData.get(counter);
				if(filteredData==null) {
					logger.warn("The reference '"+reference+"' that was provided has no data in the fetched document "+dataChunk);
				}else {
					dataReferencesSolved.put(reference, filteredData);
				}
			}
			
		}
		String instantiatedEE = expression.instantiateExpression(dataReferencesSolved);
		if(instantiatedEE!=null) {
			instantiatedEE = HelioMaterialiser.EVALUATOR.evaluateExpresions(instantiatedEE);
		}
		return instantiatedEE;
	}







	





	




	
}
