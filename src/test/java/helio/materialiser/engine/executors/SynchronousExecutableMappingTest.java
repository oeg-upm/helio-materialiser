package helio.materialiser.engine.executors;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

import org.apache.jena.rdf.model.Model;
import org.junit.Assert;
import org.junit.Test;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

import helio.framework.expressions.EvaluableExpression;
import helio.framework.materialiser.mappings.DataHandler;
import helio.framework.materialiser.mappings.DataProvider;
import helio.framework.materialiser.mappings.DataSource;
import helio.framework.materialiser.mappings.Rule;
import helio.framework.materialiser.mappings.RuleSet;
import helio.framework.objects.SparqlResultsFormat;
import helio.materialiser.configuration.HelioConfiguration;
import helio.materialiser.data.handlers.JsonHandler;
import helio.materialiser.data.providers.FileProvider;
import helio.materialiser.executors.SynchronousExecutableMapping;

public class SynchronousExecutableMappingTest {
	
	
	@Test
	public void testAddData() throws IOException {
		HelioConfiguration.HELIO_CACHE.deleteGraphs();
		
		Assert.assertTrue(HelioConfiguration.HELIO_CACHE.getGraphs().isEmpty());
				
		JsonObject object1 = (new Gson()).fromJson("{\"file\" : \"./src/test/resources/handlers-tests/json/json-file-1.json\"}", JsonObject.class);
		DataProvider memoryProvider = new FileProvider(object1);
		JsonObject object = (new Gson()).fromJson("{\"iterator\" : \"$.book[*]\"}", JsonObject.class);
		DataHandler jsonHandler = new JsonHandler();
		jsonHandler.configure(object);
		DataSource ds = new DataSource("test1", jsonHandler, memoryProvider, null);
		
		
		SynchronousExecutableMapping syncExec = new SynchronousExecutableMapping(ds, instantiateRuleSet());
		syncExec.generateRDFSynchronously();
		
		Assert.assertFalse(HelioConfiguration.HELIO_CACHE.getGraphs().isEmpty());
		HelioConfiguration.HELIO_CACHE.deleteGraphs();
	}
	
	@Test
	public void testAddDataAndQuery() throws IOException, InterruptedException {
		HelioConfiguration.HELIO_CACHE.deleteGraphs();
		
		Assert.assertTrue(HelioConfiguration.HELIO_CACHE.getGraphs().isEmpty());
				
		JsonObject object1 = (new Gson()).fromJson("{\"file\" : \"./src/test/resources/handlers-tests/json/json-file-1.json\"}", JsonObject.class);
		DataProvider memoryProvider = new FileProvider(object1);
		JsonObject object = (new Gson()).fromJson("{\"iterator\" : \"$.book[*]\"}", JsonObject.class);
		DataHandler jsonHandler = new JsonHandler();
		jsonHandler.configure(object);
		DataSource ds = new DataSource("test1", jsonHandler, memoryProvider, null);
		
		
		SynchronousExecutableMapping syncExec = new SynchronousExecutableMapping(ds, instantiateRuleSet());
		syncExec.generateRDFSynchronously();
		
		String  input = HelioConfiguration.HELIO_CACHE.solveTupleQuery("SELECT DISTINCT ?s { ?s ?p ?o .}", SparqlResultsFormat.JSON);
		
		Assert.assertFalse(input.isEmpty());
		Assert.assertFalse(HelioConfiguration.HELIO_CACHE.getGraphs().isEmpty());
		HelioConfiguration.HELIO_CACHE.deleteGraphs();
	}
	
	
	private List<RuleSet> instantiateRuleSet() {
		List<RuleSet> results = new ArrayList<>();
		RuleSet ruleSet = new RuleSet();
		ruleSet.setResourceRuleId("This is the id");
		ruleSet.setSubjectTemplate(new EvaluableExpression("http://test-1.com/book/1") );
		Set<String> arrayList = new HashSet<>();
		arrayList.add("test1");
		ruleSet.setDatasourcesId(arrayList);
		// add properties
		Rule rule = new Rule();
		rule.setPredicate(new EvaluableExpression("http://xmlns.com/foaf/0.1/name"));
		rule.setObject(new EvaluableExpression("{$.title}") );
		rule.setIsLiteral(true);
		
		Rule rule2 = new Rule();
		rule2.setPredicate(new EvaluableExpression("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"));
		rule2.setObject(new EvaluableExpression("http://xmlns.com/foaf/0.1/Document") );
		rule2.setIsLiteral(false);
		
		ruleSet.getProperties().add(rule);
		ruleSet.getProperties().add(rule2);
		results.add(ruleSet);
		
		return results;
	}
	
	
	@Test
	public void testAddDataParallel() throws IOException, InterruptedException {
		
		HelioConfiguration.HELIO_CACHE.deleteGraphs();
		Thread.sleep(1000);
		
		
		JsonObject object1 = (new Gson()).fromJson("{\"file\" : \"./src/test/resources/handlers-tests/json/json-file-1.json\"}", JsonObject.class);
		DataProvider memoryProvider = new FileProvider(object1);
		JsonObject object2 = (new Gson()).fromJson("{\"iterator\" : \"$.book[*]\"}", JsonObject.class);
		DataHandler jsonHandler1 = new JsonHandler();
		jsonHandler1.configure(object2);
		DataHandler jsonHandler2 = new JsonHandler();
		jsonHandler2.configure(object2);
		DataSource ds1 = new DataSource("test1", jsonHandler1, memoryProvider, null);
		DataSource ds2 = new DataSource("test2", jsonHandler2, memoryProvider, null);
		
		
		SynchronousExecutableMapping syncExec1 = new SynchronousExecutableMapping(ds1, instantiateRuleSet2());
		SynchronousExecutableMapping syncExec2 = new SynchronousExecutableMapping(ds2, instantiateRuleSet2());

		ForkJoinPool forkJoinPool = ForkJoinPool.commonPool();
		forkJoinPool.submit(syncExec1);
		forkJoinPool.submit(syncExec2);
		
		forkJoinPool.shutdown();
		forkJoinPool.awaitTermination(5000, TimeUnit.DAYS);
		
		Model model = HelioConfiguration.HELIO_CACHE.getGraphs();
		Assert.assertFalse(model.isEmpty());
		HelioConfiguration.HELIO_CACHE.deleteGraphs();
	}
	
	@Test
	public void testAddDataParallelAndQuery() throws IOException, InterruptedException {
		HelioConfiguration.HELIO_CACHE.deleteGraphs();
		
		Assert.assertTrue(HelioConfiguration.HELIO_CACHE.getGraphs().isEmpty());
		
		JsonObject object1 = (new Gson()).fromJson("{\"file\" : \"./src/test/resources/handlers-tests/json/json-file-1.json\"}", JsonObject.class);
		DataProvider memoryProvider = new FileProvider(object1);
		JsonObject object2 = (new Gson()).fromJson("{\"iterator\" : \"$.book[*]\"}", JsonObject.class);
		DataHandler jsonHandler1 = new JsonHandler();
		jsonHandler1.configure(object2);
		DataHandler jsonHandler2 = new JsonHandler();
		jsonHandler2.configure(object2);
		DataSource ds1 = new DataSource("test1", jsonHandler1, memoryProvider, null);
		DataSource ds2 = new DataSource("test2", jsonHandler2, memoryProvider, null);
		
		
		SynchronousExecutableMapping syncExec1 = new SynchronousExecutableMapping(ds1, instantiateRuleSet2());
		SynchronousExecutableMapping syncExec2 = new SynchronousExecutableMapping(ds2, instantiateRuleSet2());

		ForkJoinPool forkJoinPool = ForkJoinPool.commonPool();
		forkJoinPool.submit(syncExec1);
		forkJoinPool.submit(syncExec2);
		forkJoinPool.shutdown();
		forkJoinPool.awaitTermination(5000, TimeUnit.DAYS);
		String  input = HelioConfiguration.HELIO_CACHE.solveTupleQuery("SELECT DISTINCT ?s { ?s ?p ?o .}", SparqlResultsFormat.JSON);
		
		Model model = HelioConfiguration.HELIO_CACHE.getGraphs();
		Assert.assertTrue(!input.isEmpty());
		Assert.assertFalse(model.isEmpty());
		HelioConfiguration.HELIO_CACHE.deleteGraphs();
	}
	
	private List<RuleSet> instantiateRuleSet2() {
		List<RuleSet> results = new ArrayList<>();
		RuleSet ruleSet = new RuleSet();
		ruleSet.setResourceRuleId("This is the id");
		ruleSet.setSubjectTemplate(new EvaluableExpression("http://test-1.com/book/1") );
		Set<String> arrayList = new HashSet<>();
		arrayList.add("test1");
		arrayList.add("test2");
		ruleSet.setDatasourcesId(arrayList);
		// add properties
		Rule rule = new Rule();
		rule.setPredicate(new EvaluableExpression("http://xmlns.com/foaf/0.1/name"));
		rule.setObject(new EvaluableExpression("{$.title}") );
		rule.setIsLiteral(true);
		
		Rule rule2 = new Rule();
		rule2.setPredicate(new EvaluableExpression("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"));
		rule2.setObject(new EvaluableExpression("http://xmlns.com/foaf/0.1/Document") );
		rule2.setIsLiteral(false);
		
		ruleSet.getProperties().add(rule);
		ruleSet.getProperties().add(rule2);
		results.add(ruleSet);
		
		return results;
	}
}
