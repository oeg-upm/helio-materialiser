package helio.materialiser.engine.executors;

import java.io.IOException;
import java.io.PipedInputStream;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import helio.framework.materialiser.mappings.DataHandler;
import helio.framework.materialiser.mappings.DataProvider;
import helio.framework.materialiser.mappings.DataSource;
import helio.framework.materialiser.mappings.EvaluableExpression;
import helio.framework.materialiser.mappings.Rule;
import helio.framework.objects.SparqlResultsFormat;
import helio.materialiser.HelioMaterialiser;
import helio.materialiser.data.handlers.JsonHandler;
import helio.materialiser.data.providers.FileProvider;
import helio.materialiser.executors.ExecutableRule;

import org.junit.Assert;
import org.junit.Test;

public class ExecutableRuletTest {

	private static final String DATA_FRAGMENT_1 = 	"{\n" + 
			"            \"title\": \"JSON: Questions and Answers\",\n" + 
			"            \"author\": \"George Duckett\",\n" + 
			"            \"price\": 6.00\n" + 
			"        }" ;
	
	private static final String DATA_FRAGMENT_2 = 	"{\n" + 
			"            \"title\": \"JData for beginers\",\n" + 
			"            \"author\": \"George Foreman\",\n" + 
			"            \"price\": 72.00\n" + 
			"        }" ;
	
	
	
	
	@Test
	public void testAddData() throws IOException {
		HelioMaterialiser.HELIO_CACHE.deleteGraphs();
		
		Assert.assertTrue(HelioMaterialiser.HELIO_CACHE.getGraphs().isEmpty());
		
		JsonObject object1 = (new Gson()).fromJson("{\"file\" : \"./src/test/resources/json-file-1.json\"}", JsonObject.class);
		DataProvider memoryProvider = new FileProvider(object1);
		JsonObject object = (new Gson()).fromJson("{\"iterator\" : \"$.book[*]\"}", JsonObject.class);
		DataHandler jsonHandler = new JsonHandler(object);
		DataSource ds = new DataSource("test1", jsonHandler, memoryProvider, null);
		
		Rule rule = new Rule();
		rule.setPredicate(new EvaluableExpression("http://xmlns.com/foaf/0.1/name"));
		rule.setObject(new EvaluableExpression("{$.title}") );
		rule.setIsLiteral(true);
		ExecutableRule exec = new ExecutableRule("http://test-1.com/book/1", rule, ds, DATA_FRAGMENT_1);
		
		exec.generateRDF();
		Assert.assertFalse(HelioMaterialiser.HELIO_CACHE.getGraphs().isEmpty());
	}
	
	
	@Test
	public void testAddDataParallel() throws IOException, InterruptedException {
		HelioMaterialiser.HELIO_CACHE.deleteGraphs();
		
		Assert.assertTrue(HelioMaterialiser.HELIO_CACHE.getGraphs().isEmpty());
		
		JsonObject object1 = (new Gson()).fromJson("{\"file\" : \"./src/test/resources/json-file-1.json\"}", JsonObject.class);
		DataProvider memoryProvider = new FileProvider(object1);
		JsonObject object = (new Gson()).fromJson("{\"iterator\" : \"$.book[*]\"}", JsonObject.class);
		DataHandler jsonHandler = new JsonHandler(object);
		DataSource ds = new DataSource("test1", jsonHandler, memoryProvider, null);
		
		Rule rule = new Rule();
		rule.setPredicate(new EvaluableExpression("http://xmlns.com/foaf/0.1/name"));
		rule.setObject(new EvaluableExpression("{$.title}") );
		rule.setIsLiteral(true);
		ExecutableRule exec1 = new ExecutableRule("http://test-1.com/book/1", rule, ds, DATA_FRAGMENT_1);
		ExecutableRule exec2 = new ExecutableRule("http://test-1.com/book/2", rule, ds, DATA_FRAGMENT_2);
		
		ForkJoinPool forkJoinPool = ForkJoinPool.commonPool();
		forkJoinPool.execute(exec1);
		forkJoinPool.execute(exec2);
		
		forkJoinPool.awaitTermination(500, TimeUnit.DAYS);
		Assert.assertFalse(HelioMaterialiser.HELIO_CACHE.getGraphs().isEmpty());
	}
	
	@Test
	public void testAddDataAndQuery() throws IOException {
		HelioMaterialiser.HELIO_CACHE.deleteGraphs();
		
		Assert.assertTrue(HelioMaterialiser.HELIO_CACHE.getGraphs().isEmpty());
				
		JsonObject object1 = (new Gson()).fromJson("{\"file\" : \"./src/test/resources/json-file-1.json\"}", JsonObject.class);
		DataProvider memoryProvider = new FileProvider(object1);
		JsonObject object = (new Gson()).fromJson("{\"iterator\" : \"$.book[*]\"}", JsonObject.class);
		DataHandler jsonHandler = new JsonHandler(object);
		DataSource ds = new DataSource("test1", jsonHandler, memoryProvider, null);
		
		Rule rule = new Rule();
		rule.setPredicate(new EvaluableExpression("http://xmlns.com/foaf/0.1/name"));
		rule.setObject(new EvaluableExpression("{$.title}") );
		rule.setIsLiteral(true);
		ExecutableRule exec = new ExecutableRule("http://test-1.com/book/1", rule, ds, DATA_FRAGMENT_1);
		
		exec.generateRDF();
		
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
	
	@Test
	public void testAddDataParallelAndQuery() throws IOException, InterruptedException {
		HelioMaterialiser.HELIO_CACHE.deleteGraphs();
		
		Assert.assertTrue(HelioMaterialiser.HELIO_CACHE.getGraphs().isEmpty());

		JsonObject object1 = (new Gson()).fromJson("{\"file\" : \"./src/test/resources/json-file-1.json\"}", JsonObject.class);
		DataProvider memoryProvider = new FileProvider(object1);
		JsonObject object = (new Gson()).fromJson("{\"iterator\" : \"$.book[*]\"}", JsonObject.class);
		DataHandler jsonHandler = new JsonHandler(object);
		DataSource ds = new DataSource("test1", jsonHandler, memoryProvider, null);
		
		Rule rule = new Rule();
		rule.setPredicate(new EvaluableExpression("http://xmlns.com/foaf/0.1/name"));
		rule.setObject(new EvaluableExpression("{$.title}") );
		rule.setIsLiteral(true);
		ExecutableRule exec1 = new ExecutableRule("http://test-1.com/book/1", rule, ds, DATA_FRAGMENT_1);
		ExecutableRule exec2 = new ExecutableRule("http://test-1.com/book/2", rule, ds, DATA_FRAGMENT_2);
		
		ForkJoinPool forkJoinPool = ForkJoinPool.commonPool();
		forkJoinPool.execute(exec1);
		forkJoinPool.execute(exec2);
		
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
	
	
	
}
