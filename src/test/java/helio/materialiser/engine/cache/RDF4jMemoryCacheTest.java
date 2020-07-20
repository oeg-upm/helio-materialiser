package helio.materialiser.engine.cache;

import java.io.File;
import java.io.IOException;
import java.io.PipedInputStream;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.junit.Assert;
import org.junit.Test;

import helio.framework.materialiser.MaterialiserCache;
import helio.framework.objects.SparqlResultsFormat;
import helio.materialiser.cache.RDF4JMemoryCache;
import helio.materialiser.configuration.HelioConfiguration;

public class RDF4jMemoryCacheTest {
	private static Logger logger = LogManager.getLogger(RDF4jMemoryCacheTest.class);

	private static final String EXAMPLE_RDF_FRAMGMENT_1 = "<http://www.w3.org/People/EM/contact#FM> <http://www.w3.org/2000/10/swap/pim/contact#fullName> \"Frank Miller\";\n" + 
			"	<http://www.w3.org/2000/10/swap/pim/contact#personalTitle> \"Mr.\" ;\n" + 
			"	<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2000/10/swap/pim/contact#Person> .";
	
	private static final String EXAMPLE_RDF_FRAMGMENT_2 = "<http://www.w3.org/People/EM/contact#me> <http://www.w3.org/2000/10/swap/pim/contact#fullName> \"Eric Miller\";\n" + 
			"	<http://www.w3.org/2000/10/swap/pim/contact#mailbox> <mailto:e.miller123(at)example> ;\n" + 
			"	<http://www.w3.org/2000/10/swap/pim/contact#personalTitle> \"Dr.\" ;\n" + 
			"	<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2000/10/swap/pim/contact#Person> .";
	
	@Test
	public void testReadWriteOneGraph() {
		MaterialiserCache cache = new RDF4JMemoryCache();
		cache.addGraph("http://test.com/good", EXAMPLE_RDF_FRAMGMENT_1, RDFFormat.TURTLE);
		Model model = cache.getGraph("http://test.com/good");
		Assert.assertFalse(model.isEmpty());
	}
	
	
	@Test
	public void testWriteMultipleGraphsReadALL() {
		RDF4JMemoryCache cache = new RDF4JMemoryCache();
		cache.addGraph("http://test.com/good1", EXAMPLE_RDF_FRAMGMENT_1, RDFFormat.TURTLE);
		cache.addGraph("http://test.com/good2", EXAMPLE_RDF_FRAMGMENT_2, RDFFormat.TURTLE);
		Model model = cache.getGraphs();
		Boolean correctData = !model.filter(cache.createIRI("http://www.w3.org/People/EM/contact#me"), null, null).isEmpty();
		correctData &=!model.filter(cache.createIRI("http://www.w3.org/People/EM/contact#FM"), null, null).isEmpty();
		Assert.assertTrue(correctData);
	}
	
	
	@Test
	public void testReadWriteMultipleGraphs() {
		RDF4JMemoryCache cache = new RDF4JMemoryCache();
		cache.addGraph("http://test.com/good1", EXAMPLE_RDF_FRAMGMENT_1, RDFFormat.TURTLE);
		cache.addGraph("http://test.com/good2", EXAMPLE_RDF_FRAMGMENT_2, RDFFormat.TURTLE);
		String[] iris = new String[] {"http://test.com/good1", "http://test.com/good2"};
		Model model = cache.getGraphs(iris);
		Boolean correctData = !model.filter(cache.createIRI("http://www.w3.org/People/EM/contact#me"), null, null).isEmpty();
		correctData &=!model.filter(cache.createIRI("http://www.w3.org/People/EM/contact#FM"), null, null).isEmpty();
		Assert.assertTrue(correctData);
	}
	
	@Test
	public void testReadWriteMultipleGraphsMultipleThreads() {
		RDF4JMemoryCache cache = new RDF4JMemoryCache();

		Runnable r1 = new Runnable(){	 
			@Override
			public void run() {
				cache.addGraph("http://test.com/good1", EXAMPLE_RDF_FRAMGMENT_1, RDFFormat.TURTLE);
				Thread.currentThread().setName("Storing fragment 1");
			};
		};
		Runnable r2 = new Runnable(){	 
			@Override
			public void run() {
				cache.addGraph("http://test.com/good2", EXAMPLE_RDF_FRAMGMENT_2, RDFFormat.TURTLE);
				Thread.currentThread().setName("Storing fragment 2");
			};
		};
		Runnable r3 = new Runnable(){	 
			@Override
			public void run() {
				cache.getGraph("http://test.com/good1");
				Thread.currentThread().setName("Reading fragment 1");
			};
		};
		Runnable r4 = new Runnable(){	 
			@Override
			public void run() {
				cache.getGraph("http://test.com/good2");
				Thread.currentThread().setName("Reading fragment 2");
			};
		};
		Runnable r5 = new Runnable(){	 
			@Override
			public void run() {
				String[] iris = new String[] {"http://test.com/good1", "http://test.com/good2"};
				cache.getGraphs(iris);
				Thread.currentThread().setName("Reading fragment 1+2");
			};
		};
		Thread t1= new Thread(r1); 
		t1.start();
		Thread t2= new Thread(r2); 
		t2.start();
		Thread t3= new Thread(r3); 
		t3.start();
		Thread t4= new Thread(r4); 
		t4.start();
		Thread t5= new Thread(r5); 
		t5.start();
		String[] iris = new String[] {"http://test.com/good1", "http://test.com/good2"};
		
		while(t5.isAlive() || t4.isAlive() || t3.isAlive() || t2.isAlive() || t1.isAlive()) {
			
		}
		Model model = cache.getGraphs(iris);
		Boolean correctData = !model.filter(cache.createIRI("http://www.w3.org/People/EM/contact#me"), null, null).isEmpty();
		correctData &=!model.filter(cache.createIRI("http://www.w3.org/People/EM/contact#FM"), null, null).isEmpty();
		Assert.assertTrue(correctData);
	}
	
	@Test
	public void query() throws IOException {
		MaterialiserCache cache = new RDF4JMemoryCache();
		cache.addGraph("http://test.com/good1", EXAMPLE_RDF_FRAMGMENT_1, RDFFormat.TURTLE);
		cache.addGraph("http://test.com/good2", EXAMPLE_RDF_FRAMGMENT_2, RDFFormat.TURTLE);
		
		
		String query = "SELECT ?type { ?s a ?type .}";	
		PipedInputStream  input  = cache.solveTupleQuery(query, SparqlResultsFormat.JSON);
		int data = input.read();
		while(data != -1){
            System.out.print((char) data);
            data = input.read();
        }
		input.close();
	}
	
	
	@Test
	public void multipleQueries() throws InterruptedException, ExecutionException {
		RDF4JMemoryCache cache = new RDF4JMemoryCache(new File("./rdf4j-test"));
		Runnable r1 = new Runnable(){	 
			@Override
			public void run() {
				
				cache.addGraph("http://test.com/good1", EXAMPLE_RDF_FRAMGMENT_1, RDFFormat.TURTLE);
				Thread.currentThread().setName("Storing fragment 1");
			};
		};
		Runnable r2 = new Runnable(){	 
			@Override
			public void run() {
				cache.addGraph("http://test.com/good2", EXAMPLE_RDF_FRAMGMENT_2, RDFFormat.TURTLE);
				Thread.currentThread().setName("Storing fragment 2");
			};
		};
		Runnable r3 = new Runnable(){	 
			@Override
			public void run() {
				cache.getGraph("http://test.com/good1");
				Thread.currentThread().setName("Reading fragment 1");
				try {
					String query = "SELECT ?type { ?s a ?type .}";	
					PipedInputStream  input  = cache.solveTupleQuery(query, SparqlResultsFormat.JSON);
					int data = input.read();
					
					while(data != -1){
			            System.out.print((char) data);
			            data = input.read();
			        }
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			};
		};
		Runnable r4 = new Runnable(){	 
			@Override
			public void run() {
				cache.getGraph("http://test.com/good2");
				Thread.currentThread().setName("Reading fragment 2");
				try {
					String query = "SELECT ?type { ?s a ?type .}";	
					PipedInputStream  input  = cache.solveTupleQuery(query, SparqlResultsFormat.JSON);
					int data = input.read();
					
					while(data != -1){
			            System.out.print((char) data);
			            data = input.read();
			        }
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			};
		};
		Runnable r5 = new Runnable(){	 
			@Override
			public void run() {
				String[] iris = new String[] {"http://test.com/good1", "http://test.com/good2"};
				cache.getGraphs(iris);
				Thread.currentThread().setName("Reading fragment 1+2");
			};
		};
		final ForkJoinPool pool = ForkJoinPool.commonPool();
		pool.execute(r1);
		pool.execute(r2);
		pool.execute(r3);
		pool.execute(r4);
		pool.execute(r5);

		
		try {
			if(HelioConfiguration.WAIT_FOR_SYNCHRONOUS_TRANSFORMATION_TO_FINISH)
				pool.awaitTermination(99, TimeUnit.DAYS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("wait for it");
		System.out.println("-------");
		try {

			String query = "SELECT ?type { ?s a ?type .}";	
			PipedInputStream  input  = cache.solveTupleQuery(query, SparqlResultsFormat.JSON);
			int data = input.read();
			
			while(data != -1){
	            System.out.print((char) data);
	            data = input.read();
	        }
			input.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	@Test
	public void multipleQueries2() throws InterruptedException, ExecutionException {
		RDF4JMemoryCache cache = new RDF4JMemoryCache(new File("./rdf4j-test-5"));
		cache.addGraph("http://test.com/good1", EXAMPLE_RDF_FRAMGMENT_1, RDFFormat.TURTLE);
		Runnable r1 = new Runnable(){	 
			@Override
			public void run() {
				cache.addGraph("http://test.com/good1", EXAMPLE_RDF_FRAMGMENT_1, RDFFormat.TURTLE);
				Thread.currentThread().setName("Storing fragment 1");
			};
		};
		Runnable r2 = new Runnable(){	 
			@Override
			public void run() {
				cache.addGraph("http://test.com/good1", EXAMPLE_RDF_FRAMGMENT_1, RDFFormat.TURTLE);
				//cache.addStatement("http://test.com/good2", "http://test.com/good2", "http://test.com/good2", "http://test.com/good2", null, null, false);
				Thread.currentThread().setName("Storing fragment 2");
			};
		};
		
		Runnable r4 = new Runnable(){	 
			@Override
			public void run() {
				cache.addGraph("http://test.com/good1", EXAMPLE_RDF_FRAMGMENT_2, RDFFormat.TURTLE);
				Thread.currentThread().setName("Storing fragment 2");
			};
		};
		
		final ForkJoinPool pool = ForkJoinPool.commonPool();
		pool.execute(r1);
		pool.execute(r2);
		pool.execute(r4);
		
		
		try {
			if(HelioConfiguration.WAIT_FOR_SYNCHRONOUS_TRANSFORMATION_TO_FINISH)
				pool.awaitTermination(99, TimeUnit.DAYS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("wait for it");
		System.out.println("-------");
		try {

			String query = "SELECT DISTINCT ?s { ?s ?p ?type .}";	
			PipedInputStream  input  = cache.solveTupleQuery(query, SparqlResultsFormat.JSON);
			int data = input.read();
			
			while(data != -1){
	            System.out.print((char) data);
	            data = input.read();
	        }
			input.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void stressTest() {
		int index = 0;
		long max = 1000;
		long startTime = System.nanoTime();
		RDF4JMemoryCache cache = new RDF4JMemoryCache(new File("./rdf4j-test-5"));

		while(index < max) {
			
			cache.deleteGraphs();
			cache.addGraph("http://test.com/good1", EXAMPLE_RDF_FRAMGMENT_1, RDFFormat.TURTLE);
			cache.addGraph("http://test.com/good1", EXAMPLE_RDF_FRAMGMENT_2, RDFFormat.TURTLE);
			
			Assert.assertFalse(cache.getGraphs().isEmpty());
			
			try {

				String query = "SELECT DISTINCT ?s { ?s ?p ?type .}";	
				PipedInputStream  input  = cache.solveTupleQuery(query, SparqlResultsFormat.JSON);
				int data = input.read();
				StringBuilder builder = new StringBuilder();
				while(data != -1){
					builder.append((char) data);
		            data = input.read();
		        }
				input.close();
				Assert.assertFalse(builder.toString().isEmpty());
			} catch (IOException e) {
				e.printStackTrace();
			}
			index++;
		}
		long elapsedTime = System.nanoTime() - startTime;   
		logger.info("1000 times executed  oeprations: Delete, Add fragment 1, Add fragment 2, perform query. Execution in millis: "  + elapsedTime/1000000);
	}
}
