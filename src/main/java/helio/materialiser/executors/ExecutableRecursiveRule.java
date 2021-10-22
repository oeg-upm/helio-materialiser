//package helio.materialiser.executors;
//
//import java.io.ByteArrayInputStream;
//import java.io.InputStream;
//import java.nio.charset.Charset;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//import java.util.concurrent.RecursiveAction;
//import org.apache.jena.datatypes.BaseDatatype;
//import org.apache.jena.datatypes.RDFDatatype;
//import org.apache.jena.rdf.model.Literal;
//import org.apache.jena.rdf.model.Model;
//import org.apache.jena.rdf.model.ModelFactory;
//import org.apache.jena.rdf.model.RDFNode;
//import org.apache.jena.rdf.model.ResourceFactory;
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;
//
//import helio.framework.expressions.EvaluableExpression;
//import helio.framework.expressions.Expressions;
//import helio.framework.materialiser.mappings.DataSource;
//import helio.framework.materialiser.mappings.Rule;
//import helio.framework.materialiser.mappings.RuleSet;
//import helio.materialiser.HelioUtils;
//import helio.materialiser.configuration.HelioConfiguration;
//import helio.materialiser.data.handlers.RDFHandler;
//import helio.materialiser.exceptions.MalformedUriException;
//
///**
// * An alternative implementation of the {@link ExecutableRule}. Not currently used
// * @author Andrea Cimmino
// *
// */
//public class ExecutableRecursiveRule extends  RecursiveAction {
//
//	private static final long serialVersionUID = 1L;
//	private RuleSet ruleSet;
//	private List<String> dataFragments;
//
//	//private String id;
//	private DataSource dataSource;
//	private static Logger logger = LogManager.getLogger(ExecutableRecursiveRule.class);
//
//	
//	public ExecutableRecursiveRule(RuleSet ruleSet, DataSource dataSource, List<String> dataFragments) {
//		this.dataSource = dataSource;
//		this.ruleSet = ruleSet;
//		//this.id = dataSource.getId();
//		this.dataFragments = dataFragments;
//	}
//
//	@Override
//	protected void compute() {
//		try {
//			if(dataFragments.size()==1) {
//				if(dataSource.getDataHandler() instanceof RDFHandler && ruleSet.getProperties().isEmpty()) {
//					insertRDFdata();
//				}else {
//					throwTranslationThread(dataFragments.get(0));
//				}
//			}else {
//				List<String> list1 = dataFragments.subList(0, dataFragments.size()/2);
//				List<String> list2 = dataFragments.subList(dataFragments.size()/2, dataFragments.size());
//				if(list1.isEmpty() && list2.isEmpty()) {
//					//System.out.println("done");
//				}else if (!list1.isEmpty() && list2.isEmpty()) {
//					invokeAll(new ExecutableRecursiveRule(ruleSet, dataSource, list2));
//				}else if (list1.isEmpty() && !list2.isEmpty()) {
//					invokeAll(new ExecutableRecursiveRule(ruleSet, dataSource, list1));
//				}else {
//					invokeAll(new ExecutableRecursiveRule(ruleSet, dataSource, list1), new ExecutableRecursiveRule(ruleSet, dataSource, list2));
//				}
//			}
//			
//		}catch (Exception e) {
//			e.printStackTrace();
//		}
//		
//	}
//	
//	/**
//	 * Inserts RDF data into the repository
//	 */
//	private void insertRDFdata() {
//		try {
//			String dataChunk = dataFragments.get(0);
//			InputStream inputStream = new ByteArrayInputStream(dataChunk.getBytes(Charset.forName("UTF-8")));
//			Model model = ModelFactory.createDefaultModel();
//			model.read(inputStream, HelioConfiguration.DEFAULT_BASE_URI, HelioConfiguration.DEFAULT_RDF_FORMAT);
//			String namedGraph = HelioUtils.createGraphIdentifier(HelioConfiguration.DEFAULT_BASE_URI, this.dataSource.getId(), this.ruleSet.getResourceRuleId());
//			HelioConfiguration.HELIO_CACHE.deleteGraph(namedGraph);
//			HelioConfiguration.HELIO_CACHE.addGraph(namedGraph, model);
//			
//		}catch (Exception e) {
//			logger.error(e.toString());
//		}
//	}
//	
//	
//
//	private void throwTranslationThread(String dataFragment) throws MalformedUriException, InterruptedException {
//		
//		String subject = instantiateExpression(ruleSet.getSubjectTemplate(), dataFragment);
//		ExecutorService executor = Executors.newFixedThreadPool(HelioConfiguration.THREADS_INJECTING_DATA);
//		if(subject!=null) {
//			HelioConfiguration.HELIO_CACHE.deleteGraph(HelioUtils.createGraphIdentifier(subject, this.dataSource.getId(), this.ruleSet.getResourceRuleId()));
//			for(int index= 0; index < ruleSet.getProperties().size();index++) {
//				Rule rule = ruleSet.getProperties().get(index);
//				executor.submit( new Runnable() {
//				    @Override
//				    public void run() {
//				    		generateRDF(subject, rule,dataFragment);
//				    }
//				});
//			}
//		}else {
//			throw new MalformedUriException("Subject could not be formed due to data references specified in the subject that are not present in the data");
//		}
//		 executor.shutdown();
//	     executor.awaitTermination(HelioConfiguration.SYNCHRONOUS_TIMEOUT, HelioConfiguration.SYNCHRONOUS_TIMEOUT_TIME_UNIT);
//	     executor.shutdownNow();
//		
//	}
//	
//	public void generateRDF(String subject, Rule rule, String dataFragment) {
//		String instantiatedPredicate = instantiateExpression(rule.getPredicate(),  dataFragment);
//		String instantiatedObject = instantiateExpression(rule.getObject(),  dataFragment);
//		if(HelioUtils.isValidURL(subject)) {
//			if(HelioUtils.isValidURL(instantiatedPredicate)) {
//				if( rule.getIsLiteral() || (HelioUtils.isValidURL(instantiatedObject) && !rule.getIsLiteral())) {
//					persistRDF(rule, subject, instantiatedPredicate, instantiatedObject);
//				}else {
//					logger.error("Genrated object has syntax errors: "+instantiatedObject);
//				}
//			}else {
//				logger.error("Genrated predicate has syntax errors: "+instantiatedPredicate);
//			}
//		}else {
//			logger.error("Genrated subject has syntax errors: "+subject);
//		}
//	}
//	
//	
//	private void persistRDF(Rule rule, String subject, String instantiatedPredicate, String instantiatedObject) {
//		Model model = ModelFactory.createDefaultModel();
//		RDFNode node = createObjectJenaNode(instantiatedObject, rule);
//		model.createResource(subject).addProperty(ResourceFactory.createProperty(instantiatedPredicate), node);
//		HelioConfiguration.HELIO_CACHE.addGraph(HelioUtils.createGraphIdentifier(subject, this.dataSource.getId(), this.ruleSet.getResourceRuleId()), model);
//	}
//	
//     
//	
//     private Literal createObjectJenaNode(String instantiatedObject, String instantiatedDataType, String instantiatedLang, Rule rule) {
//    	 	Literal node = ResourceFactory.createPlainLiteral(instantiatedObject);
// 		if(rule.getDataType()!=null && instantiatedDataType != null) {
// 				if(HelioUtils.isValidURL(instantiatedDataType)) {
// 					RDFDatatype rdfDataType = new BaseDatatype(instantiatedDataType);
// 					node = ResourceFactory.createTypedLiteral(instantiatedObject, rdfDataType);
// 				}
// 				else {
// 					logger.error("Provided Datatype is not an URI, provided value: "+ instantiatedDataType);
// 				}
// 			}
// 			if(rule.getLanguage()!=null && instantiatedLang!=null) {
// 				if(instantiatedLang.startsWith("@")) {
// 					node = ResourceFactory.createLangLiteral(instantiatedObject, instantiatedLang); 
// 				}else {
// 					logger.error("Provided Lang is not valid, provided value: "+ instantiatedLang);
// 				}
// 				
// 			}
// 		return node;
//     }
//
//	
//
//
//
//
//
//	
//
//
//
//
//
//	
//
//
//
//
//	
//}
