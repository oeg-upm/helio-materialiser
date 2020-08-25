package helio.materialiser.executors;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.StringWriter;
import java.io.Writer;
import java.net.MalformedURLException;
import java.net.URL;
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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import helio.framework.materialiser.mappings.DataHandler;
import helio.framework.materialiser.mappings.DataSource;
import helio.framework.materialiser.mappings.EvaluableExpression;
import helio.framework.materialiser.mappings.LinkRule;
import helio.framework.materialiser.mappings.Rule;
import helio.framework.materialiser.mappings.RuleSet;
import helio.materialiser.HelioMaterialiser;
import helio.materialiser.configuration.HelioConfiguration;
import helio.materialiser.data.handlers.RDFHandler;
import helio.materialiser.exceptions.MalformedUriException;


public class ExecutableRule implements Callable<Void> {


	private RuleSet ruleSet;
	private String dataFragment;
	private DataHandler dataHandler;
	private List<LinkRule> sourceLinkingRules;
	private List<LinkRule> targetLinkingRules;
	private String id;
	
	private static Logger logger = LogManager.getLogger(ExecutableRule.class);

	
	public ExecutableRule(RuleSet ruleSet, DataSource dataSource, String dataFragment) {
		this.dataFragment = dataFragment;
		this.dataHandler = dataSource.getDataHandler();
		this.ruleSet = ruleSet;
		this.id = dataSource.getId();
		this.sourceLinkingRules = new ArrayList<>();
		this.targetLinkingRules =new ArrayList<>();


	}
	
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
			
		}catch (Exception e) {
			logger.error(e.toString());
		}
		return null;
	}

	private void insertRDFdata() {
		try {
			Resource[] iris = new Resource[] {};
			InputStream inputStream = new ByteArrayInputStream(dataFragment.getBytes(Charset.forName("UTF-8")));
			Model model = Rio.parse(inputStream, HelioConfiguration.DEFAULT_BASE_URI, HelioConfiguration.DEFAULT_RDF_FORMAT, iris);
			List<Resource> subjects = model.subjects().stream().filter(subject -> !(subject instanceof BNode)).collect(Collectors.toList());
			for(int index=0; index < subjects.size(); index++) {
				Resource subject = subjects.get(index);
				Writer writer = new StringWriter();
				Rio.write(model, writer, HelioConfiguration.DEFAULT_BASE_URI, HelioConfiguration.DEFAULT_RDF_FORMAT);
				String namedGraph = createGraphIdentifier(subject.stringValue());
				HelioMaterialiser.HELIO_CACHE.deleteGraph(namedGraph);
				HelioMaterialiser.HELIO_CACHE.addGraph(namedGraph, writer.toString(), RDFFormat.NTRIPLES);
			}
		}catch (Exception e) {
			logger.error(e.toString());
		}
	}
	
	private void throwTranslationThread(String dataFragment) throws MalformedUriException, InterruptedException {
		
		List<String> subjects = instantiateExpression(ruleSet.getSubjectTemplate(), dataFragment);
		ExecutorService executor = Executors.newFixedThreadPool(HelioConfiguration.THREADS_INJECTING_DATA);
		for(int index_sub =0; index_sub<subjects.size(); index_sub++) {
			String subject = subjects.get(index_sub);
			if(subject!=null) {
				HelioMaterialiser.HELIO_CACHE.deleteGraph(createGraphIdentifier(subject));
				for(int index= 0; index < ruleSet.getProperties().size();index++) {
					Rule rule = ruleSet.getProperties().get(index);
					// p3 no usar paralelismo
					//generateRDF(subject, rule);
					// p4 usar paralelismo
					executor.submit( new Runnable() {
					    @Override
					    public void run() {
					    		generateRDF(subject, rule);
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
	
	public void generateRDF(String subject, Rule rule) {
		List<String> instantiatedPredicates = instantiateExpression(rule.getPredicate(),  dataFragment);
		List<String> instantiatedObjects = instantiateExpression(rule.getObject(),  dataFragment);
		// update cache
		for(int index_a = 0; index_a < instantiatedPredicates.size(); index_a++) {
			String instantiatedPredicate = instantiatedPredicates.get(index_a);
			for(int index_b = 0; index_b < instantiatedObjects.size(); index_b++) {
				String instantiatedObject = instantiatedObjects.get(index_b);
				updateData(subject, instantiatedPredicate, instantiatedObject, rule);
			}
		}
		//
		updateLinkageEntries(subject);
	}
	
	public void updateData(String subject, String instantiatedPredicate, String instantiatedObject, Rule rule) {
		if(isValidURL(subject)) {
			if(isValidURL(instantiatedPredicate)) {
				if( rule.getIsLiteral() || (isValidURL(instantiatedObject) && !rule.getIsLiteral())) {
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
	
	// RDF generation methods
	
	private void persistRDF(Rule rule, String subject, String instantiatedPredicate, String instantiatedObject) {
		StringBuilder builder = new StringBuilder();
		builder.append("<").append(subject).append("> <").append(instantiatedPredicate);
		if(rule.getIsLiteral()) {
			Literal literal  = createLiteral(instantiatedObject);
			literal  = createLiteralTyped(literal, rule.getDataType());
			literal  = createLiteralLang(literal, rule.getLanguage());
			builder.append("> ").append(literal.toString()).append(" .");
		}else {
			builder.append("> <").append(instantiatedObject).append("> .");
		}
		
		HelioMaterialiser.HELIO_CACHE.addGraph(createGraphIdentifier(subject), builder.toString(), RDFFormat.NTRIPLES);
	}
	

	private IRI createIRI(String namedGraph) {
		ValueFactory valueFactory = SimpleValueFactory.getInstance();
		return valueFactory.createIRI(namedGraph);
	}
	
	private Literal createLiteral(String literal) {
		ValueFactory valueFactory = SimpleValueFactory.getInstance();
		return valueFactory.createLiteral(literal);
	}
	
	private Literal createLiteralTyped(Literal literal, String datatype) {
		Literal newLiteral = literal;
		if(datatype!=null) {
			ValueFactory valueFactory = SimpleValueFactory.getInstance();
			newLiteral = valueFactory.createLiteral(literal.stringValue(), createIRI(datatype));
		}
		return newLiteral;
	}
	private Literal createLiteralLang(Literal literal, String lang) {
		Literal newLiteral = literal;
		if(lang!=null) {
			ValueFactory valueFactory = SimpleValueFactory.getInstance();
			newLiteral = valueFactory.createLiteral(literal.stringValue(), lang);
		}
		return newLiteral;
	}
	
	private boolean isValidURL(String urlStr) {
	    try {
	    		if(urlStr == null || urlStr.contains(" ") || urlStr.isEmpty())
	    			throw new MalformedURLException();
	        new URL(urlStr);
	        return true;
	      }
	      catch (MalformedURLException e) {
	          return false;
	      }
	  }

	private String createGraphIdentifier(String subject) {
		StringBuilder builder = new StringBuilder();
		builder.append(subject).append("/").append(String.valueOf(this.id.hashCode()).replace("-", "0"));
		return builder.toString();
	}
	
	private List<String> instantiateExpression(EvaluableExpression expression, String dataChunk) {
		List<Map<String,String>> dataReferencesSolvedList = new ArrayList<>();
		List<String> dataReferences = expression.getDataReferences();
		for(int index=0;index < dataReferences.size(); index++) {
			String reference = dataReferences.get(index);
			List<String> setFilteredData = dataHandler.filter(reference, dataChunk);
			for(int counter=0; counter<setFilteredData.size(); counter++) {
				Map<String,String> dataReferencesSolved = new HashMap<>();
				String filteredData = setFilteredData.get(counter);
				if(filteredData==null) {
					logger.warn("The reference '"+reference+"' that was provided has no data in the fetched document "+dataChunk);
				}else {
					dataReferencesSolved.put(reference, filteredData);
					dataReferencesSolvedList.add(dataReferencesSolved);
				}
			}
		}
	
		return injectValuesInEE(expression, dataReferencesSolvedList);
	}

	private List<String> injectValuesInEE(EvaluableExpression expression, List<Map<String,String>> dataReferencesSolvedList) {
		List<String> instantiatedEEs = new ArrayList<>();
		for(int index=0; index<dataReferencesSolvedList.size(); index++) {
			Map<String,String> dataReferencesSolved = dataReferencesSolvedList.get(index);
			String instantiatedEE = expression.instantiateExpression(dataReferencesSolved);
			if(instantiatedEE!=null) {
				instantiatedEE = HelioMaterialiser.EVALUATOR.evaluateExpresions(instantiatedEE);
				instantiatedEEs.add(instantiatedEE);
			}
		}
		if(dataReferencesSolvedList.isEmpty() && expression.getDataReferences().isEmpty()) {
			String instantiatedEE = HelioMaterialiser.EVALUATOR.evaluateExpresions(expression.getExpression());
			instantiatedEEs.add(instantiatedEE);
		}
		return instantiatedEEs;
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
			HelioMaterialiser.EVALUATOR.updateCache(subject, null, subjectValuesExtracted, null, linkRule);
		}
	}
	private void updateTargetLinkageEntries(String subject) {
		for(int index=0; index < targetLinkingRules.size(); index++) {
			LinkRule linkRule = targetLinkingRules.get(index);
			// create new 
			String targetValuesExtracted =  instantiateLinkingExpression(linkRule.getExpression(), dataFragment, false);
			HelioMaterialiser.EVALUATOR.updateCache(null, subject, null, targetValuesExtracted, linkRule);
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
