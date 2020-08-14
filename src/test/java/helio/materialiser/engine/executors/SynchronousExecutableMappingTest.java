package helio.materialiser.engine.executors;

import java.io.IOException;
import java.io.PipedInputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.junit.Assert;
import org.junit.Test;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

import helio.framework.materialiser.mappings.DataHandler;
import helio.framework.materialiser.mappings.DataProvider;
import helio.framework.materialiser.mappings.DataSource;
import helio.framework.materialiser.mappings.EvaluableExpression;
import helio.framework.materialiser.mappings.Rule;
import helio.framework.materialiser.mappings.RuleSet;
import helio.framework.objects.SparqlResultsFormat;
import helio.materialiser.HelioMaterialiser;
import helio.materialiser.data.handlers.JsonHandler;
import helio.materialiser.data.providers.FileProvider;
import helio.materialiser.executors.SynchronousExecutableMapping;

public class SynchronousExecutableMappingTest {
	
	
	@Test
	public void testAddData() throws IOException {
		HelioMaterialiser.HELIO_CACHE.deleteGraphs();
		
		Assert.assertTrue(HelioMaterialiser.HELIO_CACHE.getGraphs().isEmpty());
				
		JsonObject object1 = (new Gson()).fromJson("{\"file\" : \"./src/test/resources/json-file-1.json\"}", JsonObject.class);
		DataProvider memoryProvider = new FileProvider(object1);
		JsonObject object = (new Gson()).fromJson("{\"iterator\" : \"$.book[*]\"}", JsonObject.class);
		DataHandler jsonHandler = new JsonHandler(object);
		DataSource ds = new DataSource("test1", jsonHandler, memoryProvider, null);
		
		
		SynchronousExecutableMapping syncExec = new SynchronousExecutableMapping(ds, instantiateRuleSet());
		syncExec.generateRDFSynchronously();
		
		Assert.assertFalse(HelioMaterialiser.HELIO_CACHE.getGraphs().isEmpty());
	}
	
	@Test
	public void testAddDataAndQuery() throws IOException, InterruptedException {
		HelioMaterialiser.HELIO_CACHE.deleteGraphs();
		
		Assert.assertTrue(HelioMaterialiser.HELIO_CACHE.getGraphs().isEmpty());
				
		JsonObject object1 = (new Gson()).fromJson("{\"file\" : \"./src/test/resources/json-file-1.json\"}", JsonObject.class);
		DataProvider memoryProvider = new FileProvider(object1);
		JsonObject object = (new Gson()).fromJson("{\"iterator\" : \"$.book[*]\"}", JsonObject.class);
		DataHandler jsonHandler = new JsonHandler(object);
		DataSource ds = new DataSource("test1", jsonHandler, memoryProvider, null);
		
		
		SynchronousExecutableMapping syncExec = new SynchronousExecutableMapping(ds, instantiateRuleSet());
		syncExec.generateRDFSynchronously();
		
		PipedInputStream  input = HelioMaterialiser.HELIO_CACHE.solveTupleQuery("SELECT DISTINCT ?s { ?s ?p ?o .}", SparqlResultsFormat.JSON);
		StringBuilder builder = new StringBuilder();
		int data = input.read();
		while(data != -1){
			builder.append((char) data);
            data = input.read();
        }
		input.close();
		Assert.assertFalse(builder.toString().isEmpty());
		Assert.assertFalse(HelioMaterialiser.HELIO_CACHE.getGraphs().isEmpty());
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
		HelioMaterialiser.HELIO_CACHE.deleteGraphs();
		
		Rio.write(HelioMaterialiser.HELIO_CACHE.getGraphs(), System.out, RDFFormat.NTRIPLES);
		Assert.assertTrue(HelioMaterialiser.HELIO_CACHE.getGraphs().isEmpty());
		
		
		JsonObject object1 = (new Gson()).fromJson("{\"file\" : \"./src/test/resources/json-file-1.json\"}", JsonObject.class);
		DataProvider memoryProvider = new FileProvider(object1);
		JsonObject object2 = (new Gson()).fromJson("{\"iterator\" : \"$.book[*]\"}", JsonObject.class);
		DataHandler jsonHandler1 = new JsonHandler(object2);
		DataHandler jsonHandler2 = new JsonHandler(object2);
		DataSource ds1 = new DataSource("test1", jsonHandler1, memoryProvider, null);
		DataSource ds2 = new DataSource("test2", jsonHandler2, memoryProvider, null);
		
		
		SynchronousExecutableMapping syncExec1 = new SynchronousExecutableMapping(ds1, instantiateRuleSet2());
		SynchronousExecutableMapping syncExec2 = new SynchronousExecutableMapping(ds2, instantiateRuleSet2());

		ForkJoinPool forkJoinPool = ForkJoinPool.commonPool();
		forkJoinPool.submit(syncExec1);
		forkJoinPool.submit(syncExec2);
		
		forkJoinPool.awaitTermination(500, TimeUnit.DAYS);
		forkJoinPool.shutdown();
		
		Assert.assertFalse(!HelioMaterialiser.HELIO_CACHE.getGraphs().isEmpty());
	}
	
	@Test
	public void testAddDataParallelAndQuery() throws IOException, InterruptedException {
		HelioMaterialiser.HELIO_CACHE.deleteGraphs();
		
		Rio.write(HelioMaterialiser.HELIO_CACHE.getGraphs(), System.out, RDFFormat.NTRIPLES);
		Assert.assertTrue(HelioMaterialiser.HELIO_CACHE.getGraphs().isEmpty());
		
		
		JsonObject object1 = (new Gson()).fromJson("{\"file\" : \"./src/test/resources/json-file-1.json\"}", JsonObject.class);
		DataProvider memoryProvider = new FileProvider(object1);
		JsonObject object2 = (new Gson()).fromJson("{\"iterator\" : \"$.book[*]\"}", JsonObject.class);
		DataHandler jsonHandler1 = new JsonHandler(object2);
		DataHandler jsonHandler2 = new JsonHandler(object2);
		DataSource ds1 = new DataSource("test1", jsonHandler1, memoryProvider, null);
		DataSource ds2 = new DataSource("test2", jsonHandler2, memoryProvider, null);
		
		
		SynchronousExecutableMapping syncExec1 = new SynchronousExecutableMapping(ds1, instantiateRuleSet2());
		SynchronousExecutableMapping syncExec2 = new SynchronousExecutableMapping(ds2, instantiateRuleSet2());

		ForkJoinPool forkJoinPool = ForkJoinPool.commonPool();
		forkJoinPool.submit(syncExec1);
		forkJoinPool.submit(syncExec2);
		
		forkJoinPool.awaitTermination(500, TimeUnit.DAYS);
		PipedInputStream  input = HelioMaterialiser.HELIO_CACHE.solveTupleQuery("SELECT DISTINCT ?s { ?s ?p ?o .}", SparqlResultsFormat.JSON);
		StringBuilder builder = new StringBuilder();
		int data = input.read();
		while(data != -1){
			builder.append((char) data);
            data = input.read();
        }
		input.close();
		Assert.assertFalse(builder.toString().isEmpty());
		Assert.assertFalse(HelioMaterialiser.HELIO_CACHE.getGraphs().isEmpty());
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
