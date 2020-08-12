package helio.materialiser.executors;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RecursiveAction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.rio.RDFFormat;
import helio.framework.materialiser.mappings.DataSource;
import helio.framework.materialiser.mappings.EvaluableExpression;
import helio.framework.materialiser.mappings.Rule;
import helio.framework.materialiser.mappings.RuleSet;
import helio.materialiser.HelioMaterialiser;
import helio.materialiser.configuration.HelioConfiguration;
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
				throwTranslationThread(dataFragments.get(0));
			}else {
				List<String> list1 = dataFragments.subList(0, dataFragments.size()/2);
				List<String> list2 = dataFragments.subList(dataFragments.size()/2, dataFragments.size());
				//System.out.println(dataFragments.size()+" = "+list1.size()+" + "+list2.size());
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

	private void throwTranslationThread(String dataFragment) throws MalformedUriException, InterruptedException {
		
		String subject = instantiateExpression(ruleSet.getSubjectTemplate(), dataFragment);
		ExecutorService executor = Executors.newFixedThreadPool(HelioConfiguration.THREADS_INJECTING_DATA);
		if(subject!=null) {
			HelioMaterialiser.HELIO_CACHE.deleteGraph(createGraphIdentifier(subject));
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
			instantiatedEE = HelioMaterialiser.EVALUATOR.evaluateExpresions(instantiatedEE);
		}
		return instantiatedEE;
	}







	





	




	
}
