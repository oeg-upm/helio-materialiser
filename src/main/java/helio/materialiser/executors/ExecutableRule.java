package helio.materialiser.executors;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.jena.datatypes.BaseDatatype;
import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import helio.framework.expressions.EvaluableExpression;
import helio.framework.expressions.EvaluableExpressionException;
import helio.framework.expressions.Expressions;
import helio.framework.materialiser.MaterialiserCache;
import helio.framework.materialiser.mappings.DataHandler;
import helio.framework.materialiser.mappings.DataSource;
import helio.framework.materialiser.mappings.LinkRule;
import helio.framework.materialiser.mappings.Rule;
import helio.framework.materialiser.mappings.RuleSet;
import helio.framework.objects.Utils;
import helio.materialiser.HelioUtils;
import helio.materialiser.MaterialiserOrchestrator;
import helio.materialiser.configuration.HelioConfiguration;
import helio.materialiser.data.handlers.RDFHandler;
import helio.materialiser.exceptions.MalformedUriException;

/**
 * This objects wraps a {@link Rule} extending its functionalities, instantiating the {@link Rule} with data from the {@link DataSource}, generating a set of RDF triples, and storing the generated data in the {@link MaterialiserCache}
 * @author Andrea Cimmino
 *
 */
public class ExecutableRule implements Callable<Void> {

	
	private RuleSet ruleSet;
	private String dataFragment;
	private DataHandler dataHandler;
	private List<LinkRule> sourceLinkingRules;
	private List<LinkRule> targetLinkingRules;
	private String id;
	
	private static Logger logger = LogManager.getLogger(ExecutableRule.class);

	/**
	 * This method initializes the {@link ExecutableRule} with the provided {@link RuleSet}, {@link DataSource}, and a fragment of data used to instantiate the rules from the {@link RuleSet}
	 * @param ruleSet a valid {@link RuleSet}
	 * @param dataSource a valid {@link DataSource}
	 * @param dataFragment a fragment of data expressed in a format supported by the {@link DataHandler} within the {@link DataSource}
	 */
	public ExecutableRule(RuleSet ruleSet, DataSource dataSource, String dataFragment) {
		this.dataFragment = dataFragment;
		this.dataHandler = dataSource.getDataHandler();
		this.ruleSet = ruleSet;
		this.id = dataSource.getId();
		this.sourceLinkingRules = new ArrayList<>();
		this.targetLinkingRules =new ArrayList<>();
	}
	
	/**
	 * This method initializes the {@link ExecutableRule} with the provided {@link RuleSet}, {@link DataSource}, a set of {@link LinkRule}, and a fragment of data used to instantiate the rules from the {@link RuleSet} and the {@link LinkRule}
	 * @param ruleSet a valid {@link RuleSet}
	 * @param dataSource a valid {@link DataSource}
	 * @param dataFragment a fragment of data expressed in a format supported by the {@link DataHandler} within the {@link DataSource}
	 * @param linkingRules a {@link List} of {@link LinkRule}
	 */
	public ExecutableRule(RuleSet ruleSet, DataSource dataSource, String dataFragment, List<LinkRule> linkingRules) {
		this.dataFragment = dataFragment;
		this.dataHandler = dataSource.getDataHandler();
		this.ruleSet = ruleSet;
		this.id = dataSource.getId();
		this.sourceLinkingRules = linkingRules.parallelStream().filter(linkRule -> ruleSet.getResourceRuleId().equals(linkRule.getSourceNamedGraph())).collect(Collectors.toList());
		this.targetLinkingRules = linkingRules.parallelStream().filter(linkRule -> ruleSet.getResourceRuleId().equals(linkRule.getTargetNamedGraph())).collect(Collectors.toList());
	}


	@Override
	public Void call() throws Exception {
		try {
			if(dataHandler instanceof RDFHandler && ruleSet.getProperties().isEmpty()) {
				insertRDFdata();
			}else {
				throwTranslationThread(dataFragment);
			}
		}catch (EvaluableExpressionException e){
			logger.error(Utils.buildMessage("An error ocurred creating the subject ", ruleSet.getSubjectTemplate().getExpression(), ". ", e.toString()));
		}catch (Exception e) {
			logger.error(e.toString());
		}
		return null;
	}

	/**
	 * Inserts RDF data into the repository
	 */
	private void insertRDFdata() {
		try {
			InputStream inputStream = new ByteArrayInputStream(dataFragment.getBytes(Charset.forName("UTF-8")));
			Model model = ModelFactory.createDefaultModel();
			model.read(inputStream, HelioConfiguration.DEFAULT_BASE_URI, HelioConfiguration.DEFAULT_RDF_FORMAT);
			String namedGraph = HelioUtils.createGraphIdentifier(HelioConfiguration.DEFAULT_BASE_URI, id, this.ruleSet.getResourceRuleId());
			HelioConfiguration.HELIO_CACHE.deleteGraph(namedGraph);
			HelioConfiguration.HELIO_CACHE.addGraph(namedGraph, model);
		}catch (Exception e) {
			logger.error(e.toString());
		}
	}
	
	
	
	private void throwTranslationThread(String dataFragment) throws MalformedUriException, InterruptedException, EvaluableExpressionException {
		ExecutorService executor = Executors.newFixedThreadPool(HelioConfiguration.THREADS_INJECTING_DATA);

		List<String> subjects =  Expressions.instantiate(ruleSet.getSubjectTemplate(), dataFragment, dataHandler, HelioConfiguration.EVALUATOR);
		for(int subjectIndex =0; subjectIndex<subjects.size(); subjectIndex++) {
			String subject = subjects.get(subjectIndex);
			if(subject!=null) {
				String instantiatedSubject = HelioUtils.createGraphIdentifier(subject,id, this.ruleSet.getResourceRuleId());
				if(!MaterialiserOrchestrator.updatedSynchornousSubjects.contains(instantiatedSubject)) {
					HelioConfiguration.HELIO_CACHE.deleteGraph(instantiatedSubject);
					MaterialiserOrchestrator.updatedSynchornousSubjects.add(instantiatedSubject);
				}
				for(int index= 0; index < ruleSet.getProperties().size();index++) {
					Rule rule = ruleSet.getProperties().get(index);
					// p3 no usar paralelismo
					//generateRDF(subject, rule);
					// p4 usar paralelismo
					executor.submit( new Runnable() {
					    @Override
					    public void run() {
					    		generateRDF(subject, rule);
					    		updateLinkageEntries(subject);
					    }
					});
				}
				
			}else {
				throw new MalformedUriException("Subject could not be formed due to data references specified in the subject that are not present in the data");
			}
		}
		 executor.shutdown();
	     executor.awaitTermination(HelioConfiguration.SYNCHRONOUS_TIMEOUT, HelioConfiguration.SYNCHRONOUS_TIMEOUT_TIME_UNIT);
	     executor.shutdownNow();
	     
		
	}
	
	/**
	 * This method generates the RDF related to the provided {@link Rule}, and then updates the {@link MaterialiserCache}
	 * @param subject a valid URL identifying an RDF triple
	 * @param rule a valid {@link Rule}
	 */
	public void generateRDF(String subject, Rule rule) {
		try { // TODO: separate in another method and throw a presonalised exception for each
			List<String> instantiatedPredicates = Expressions.instantiate(rule.getPredicate(),  dataFragment, dataHandler, HelioConfiguration.EVALUATOR);
			List<String> instantiatedObjects =  Expressions.instantiate(rule.getObject(),  dataFragment,  dataHandler, HelioConfiguration.EVALUATOR);
			List<String> instantiatedDataTypes = Expressions.instantiate(rule.getDataType(), dataFragment,  dataHandler, HelioConfiguration.EVALUATOR);
			List<String> instantiatedLangs =  Expressions.instantiate(rule.getLanguage(), dataFragment,  dataHandler, HelioConfiguration.EVALUATOR);
			// update cache
			for(int predicateIndex = 0; predicateIndex < instantiatedPredicates.size(); predicateIndex++) {
				String instantiatedPredicate = instantiatedPredicates.get(predicateIndex);
				for(int objectIndex = 0; objectIndex < instantiatedObjects.size(); objectIndex++) {
					String instantiatedObject = instantiatedObjects.get(objectIndex);
					updateData(subject, instantiatedPredicate, instantiatedObject, instantiatedDataTypes, instantiatedLangs, rule);
				}
			}
		} catch(Exception e) {
			logger.error(e.toString());
		}
	}
	
	private void updateData(String subject, String instantiatedPredicate, String instantiatedObject, List<String> instantiatedDataType, List<String> instantiatedLang, Rule rule) {
		if(HelioUtils.isValidURL(subject)) {
			if(HelioUtils.isValidURL(instantiatedPredicate)) {
				if( rule.getIsLiteral() || (HelioUtils.isValidURL(instantiatedObject) && !rule.getIsLiteral())) {
					persistRDF(rule, subject, instantiatedPredicate, instantiatedObject, instantiatedDataType, instantiatedLang);
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
	
	// RDF generation methods
	
	private void persistRDF(Rule rule, String subject, String instantiatedPredicate, String instantiatedObject, List<String> instantiatedDataTypes, List<String> instantiatedLangs) {
		Model model = ModelFactory.createDefaultModel();
		
		if(!rule.getIsLiteral()) {
			model.createResource(subject).addProperty(ResourceFactory.createProperty(instantiatedPredicate), ResourceFactory.createResource(instantiatedObject));
		}else {
			if(!instantiatedDataTypes.isEmpty()) {
				for(int pointerDataType = 0; pointerDataType < instantiatedDataTypes.size(); pointerDataType ++) {
					String instantiatedDataType = instantiatedDataTypes.get(pointerDataType);
					Literal node = createObjectJenaNode(instantiatedObject, instantiatedDataType, null, rule);
					model.add(ResourceFactory.createResource(subject), ResourceFactory.createProperty(instantiatedPredicate), node);
				}
			}else if(!instantiatedLangs.isEmpty()){
				for(int pointerLang = 0; pointerLang < instantiatedLangs.size(); pointerLang ++) {
					String instantiatedLang = instantiatedLangs.get(pointerLang);
					Literal node = createObjectJenaNode(instantiatedObject, null, instantiatedLang, rule);
					model.add(ResourceFactory.createResource(subject), ResourceFactory.createProperty(instantiatedPredicate), node);
				}
			}else {
				Literal node = createObjectJenaNode(instantiatedObject, null, null, rule);
				model.add(ResourceFactory.createResource(subject), ResourceFactory.createProperty(instantiatedPredicate), node);
			}
				
		}
		HelioConfiguration.HELIO_CACHE.addGraph(HelioUtils.createGraphIdentifier(subject, this.id, this.ruleSet.getResourceRuleId()), model);
	}
	
     private Literal createObjectJenaNode(String instantiatedObject, String instantiatedDataType, String instantiatedLang, Rule rule) {
    	 	Literal node = ResourceFactory.createPlainLiteral(instantiatedObject);
 		if(rule.getDataType()!=null && instantiatedDataType != null) {
 				if(HelioUtils.isValidURL(instantiatedDataType)) {
 					RDFDatatype rdfDataType = new BaseDatatype(instantiatedDataType);
 					node = ResourceFactory.createTypedLiteral(instantiatedObject, rdfDataType);
 				}
 				else {
 					logger.error("Provided Datatype is not an URI, provided value: "+ instantiatedDataType);
 				}
 			}
 			if(rule.getLanguage()!=null && instantiatedLang!=null) {
 				if(!instantiatedLang.isEmpty()) {
 					node = ResourceFactory.createLangLiteral(instantiatedObject, instantiatedLang); 
 				}else {
 					logger.error("Provided Lang is not valid, provided value: "+ instantiatedLang);
 				}
 				
 			}
 		return node;
     }


	// Data linking methods
	
	private void updateLinkageEntries(String subject) {
		updateSourceLinkageEntries(subject);
		updateTargetLinkageEntries(subject);
	}
	
	private void updateSourceLinkageEntries(String subject) {
		for (int index = 0; index < sourceLinkingRules.size(); index++) {
			LinkRule linkRule = sourceLinkingRules.get(index);
			// create new
			String subjectValuesExtracted =  instantiateLinkingExpression(linkRule.getExpression(), dataFragment, true);
			HelioConfiguration.EVALUATOR.updateCache(subject, null, subjectValuesExtracted, null, linkRule);
		}
	}
	private void updateTargetLinkageEntries(String subject) {
		for(int index=0; index < targetLinkingRules.size(); index++) {
			LinkRule linkRule = targetLinkingRules.get(index);
			// create new 
			String targetValuesExtracted =  instantiateLinkingExpression(linkRule.getExpression(), dataFragment, false);
			HelioConfiguration.EVALUATOR.updateCache(null, subject, null, targetValuesExtracted, linkRule);
		}
	}
	
	private String instantiateLinkingExpression(EvaluableExpression expression, String dataChunk, Boolean isSource) {
		String result = null;
		List<String> dataReferences = extractDataReferences(expression.getExpression(), isSource); 
		JsonArray arrayOfLinkageMaps = new JsonArray();
		for(int index=0;index < dataReferences.size(); index++) {
			String reference = dataReferences.get(index);
			List<String> setFilteredData = dataHandler.filter(reference, dataChunk);
			for(int counter=0; counter < setFilteredData.size(); counter++) {
				String filteredData = setFilteredData.get(counter);
				JsonObject linkageMap = new JsonObject();
				linkageMap.addProperty(reference, filteredData);
				arrayOfLinkageMaps.add(linkageMap);
			}
		}
		
		if(arrayOfLinkageMaps.size()>0)
			result = arrayOfLinkageMaps.toString();
		return result;
	}

	private List<String> extractDataReferences(String expression, Boolean isSource) {
		// 1. Compile pattern to find identifiers
		String token = "T";
		if(isSource)
			token = "S";
		Pattern pattern = Pattern.compile(token+"\\(\\{[^\\}\\)]+\\}\\)");
		Matcher matcher = pattern.matcher(expression);
		// 2. Find identifiers with compiled matcher from pattern
		List<String> results = new ArrayList<>();
		while (matcher.find()) {
			String newIdentifier = matcher.group();
			// 2.1 Clean identifier found
			newIdentifier = newIdentifier.replace(token+"({", "").replace("})", "").trim();
			results.add(newIdentifier);
			
		}
		
		return results;
	}
	
	



	




	
}
