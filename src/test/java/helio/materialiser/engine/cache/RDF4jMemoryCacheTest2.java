package helio.materialiser.engine.cache;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.junit.Assert;
import org.junit.Test;

import helio.framework.objects.SparqlResultsFormat;
import helio.materialiser.configuration.HelioConfiguration;

public class RDF4jMemoryCacheTest2 {


	private static final String EXAMPLE_RDF_FRAMGMENT_1 = "<http://www.w3.org/People/EM/contact#FM> <http://www.w3.org/2000/10/swap/pim/contact#fullName> \"Frank Miller\";\n" + 
			"	<http://www.w3.org/2000/10/swap/pim/contact#personalTitle> \"Mr.\" ;\n" + 
			"	<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2000/10/swap/pim/contact#Person> .";
	
	private static final String EXAMPLE_RDF_FRAMGMENT_2 = "<http://www.w3.org/People/EM/contact#me> <http://www.w3.org/2000/10/swap/pim/contact#fullName> \"Eric Miller\";\n" + 
			"	<http://www.w3.org/2000/10/swap/pim/contact#mailbox> <mailto:e.miller123(at)example> ;\n" + 
			"	<http://www.w3.org/2000/10/swap/pim/contact#personalTitle> \"Dr.\" ;\n" + 
			"	<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2000/10/swap/pim/contact#Person> .";
	private static final Boolean WAIT_FOR_SYNCHRONOUS_TRANSFORMATION_TO_FINISH = true;

	@Test
	public void testReadWriteOneGraph() throws IOException {
		HelioConfiguration.HELIO_CACHE.deleteGraphs();
		Model model = ModelFactory.createDefaultModel().read(IOUtils.toInputStream(EXAMPLE_RDF_FRAMGMENT_1, "UTF-8"), HelioConfiguration.DEFAULT_BASE_URI, "TURTLE");
		HelioConfiguration.HELIO_CACHE.addGraph("http://test.com/good", model);
		Model model2 = HelioConfiguration.HELIO_CACHE.getGraph("http://test.com/good");
		Assert.assertFalse(model2.isEmpty());
		HelioConfiguration.HELIO_CACHE.deleteGraphs();
	}
	
	
	@Test
	public void testWriteMultipleGraphsReadALL() throws IOException {
		HelioConfiguration.HELIO_CACHE.deleteGraphs();
		Model model1 = ModelFactory.createDefaultModel().read(IOUtils.toInputStream(EXAMPLE_RDF_FRAMGMENT_1, "UTF-8"), HelioConfiguration.DEFAULT_BASE_URI, "TURTLE");
		Model model2 = ModelFactory.createDefaultModel().read(IOUtils.toInputStream(EXAMPLE_RDF_FRAMGMENT_1, "UTF-8"), HelioConfiguration.DEFAULT_BASE_URI, "TURTLE");
		HelioConfiguration.HELIO_CACHE.addGraph("http://test.com/good1", model1);
		HelioConfiguration.HELIO_CACHE.addGraph("http://test.com/good2", model2);
		Model model = HelioConfiguration.HELIO_CACHE.getGraph("http://test.com/good1");
		Assert.assertFalse(model.isEmpty());
		HelioConfiguration.HELIO_CACHE.deleteGraphs();
	}
	
	
	@Test
	public void testReadWriteMultipleGraphs() throws IOException {
		HelioConfiguration.HELIO_CACHE.deleteGraphs();
		Model model1 = ModelFactory.createDefaultModel().read(IOUtils.toInputStream(EXAMPLE_RDF_FRAMGMENT_1, "UTF-8"), HelioConfiguration.DEFAULT_BASE_URI, "TURTLE");
		Model model2 = ModelFactory.createDefaultModel().read(IOUtils.toInputStream(EXAMPLE_RDF_FRAMGMENT_1, "UTF-8"), HelioConfiguration.DEFAULT_BASE_URI, "TURTLE");
		HelioConfiguration.HELIO_CACHE.addGraph("http://test.com/good1", model1);
		HelioConfiguration.HELIO_CACHE.addGraph("http://test.com/good2", model2);
		Model model = HelioConfiguration.HELIO_CACHE.getGraph("http://test.com/good"); // notice that this graph does not exists
		Assert.assertTrue(model.isEmpty());
		HelioConfiguration.HELIO_CACHE.deleteGraphs();
	}
	
	@Test
	public void testReadWriteMultipleGraphsMultipleThreads() {
		HelioConfiguration.HELIO_CACHE.deleteGraphs();

		Runnable r1 = new Runnable(){	 
			@Override
			public void run() {
				try {
				Model model1 = ModelFactory.createDefaultModel().read(IOUtils.toInputStream(EXAMPLE_RDF_FRAMGMENT_1, "UTF-8"), HelioConfiguration.DEFAULT_BASE_URI, "TURTLE");
				HelioConfiguration.HELIO_CACHE.addGraph("http://test.com/good1", model1);
				Thread.currentThread().setName("Storing fragment 1");
				}catch(Exception e) {
					e.printStackTrace();
				}
			};
		};
		Runnable r2 = new Runnable(){	 
			@Override
			public void run() {
				try {
					Model model1 = ModelFactory.createDefaultModel().read(IOUtils.toInputStream(EXAMPLE_RDF_FRAMGMENT_2, "UTF-8"), HelioConfiguration.DEFAULT_BASE_URI, "TURTLE");
					HelioConfiguration.HELIO_CACHE.addGraph("http://test.com/good2", model1);
					Thread.currentThread().setName("Storing fragment 1");
					}catch(Exception e) {
						e.printStackTrace();
					}
				Thread.currentThread().setName("Storing fragment 2");
			};
		};
		Runnable r3 = new Runnable(){	 
			@Override
			public void run() {
				HelioConfiguration.HELIO_CACHE.getGraph("http://test.com/good1");
				Thread.currentThread().setName("Reading fragment 1");
			};
		};
		Runnable r4 = new Runnable(){	 
			@Override
			public void run() {
				HelioConfiguration.HELIO_CACHE.getGraph("http://test.com/good2");
				Thread.currentThread().setName("Reading fragment 2");
			};
		};
		Runnable r5 = new Runnable(){	 
			@Override
			public void run() {
				String[] iris = new String[] {"http://test.com/good1", "http://test.com/good2"};
				HelioConfiguration.HELIO_CACHE.getGraphs(iris);
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
		Model model = HelioConfiguration.HELIO_CACHE.getGraphs(iris);
		Assert.assertFalse(model.isEmpty());
		HelioConfiguration.HELIO_CACHE.deleteGraphs();
	}
	
	@Test
	public void query() throws IOException {
		HelioConfiguration.HELIO_CACHE.deleteGraphs();
	
		Model model1 = ModelFactory.createDefaultModel().read(IOUtils.toInputStream(EXAMPLE_RDF_FRAMGMENT_1, "UTF-8"), HelioConfiguration.DEFAULT_BASE_URI, "TURTLE");
		HelioConfiguration.HELIO_CACHE.addGraph("http://test.com/good1", model1);
		Model model2 = ModelFactory.createDefaultModel().read(IOUtils.toInputStream(EXAMPLE_RDF_FRAMGMENT_2, "UTF-8"), HelioConfiguration.DEFAULT_BASE_URI, "TURTLE");
		HelioConfiguration.HELIO_CACHE.addGraph("http://test.com/good2", model2);
			
		
		String query = "SELECT ?type { ?s a ?type .}";	
		String  input  = HelioConfiguration.HELIO_CACHE.solveTupleQuery(query, SparqlResultsFormat.JSON);
		Assert.assertFalse(input.isEmpty());
	}
	
	
	@Test
	public void multipleQueries() throws InterruptedException, ExecutionException {
		Runnable r1 = new Runnable(){	 
			@Override
			public void run() {
				try {
					Model model1 = ModelFactory.createDefaultModel().read(IOUtils.toInputStream(EXAMPLE_RDF_FRAMGMENT_1, "UTF-8"), HelioConfiguration.DEFAULT_BASE_URI, "TURTLE");
					HelioConfiguration.HELIO_CACHE.addGraph("http://test.com/good1", model1);
					Thread.currentThread().setName("Storing fragment 1");
					}catch(Exception e) {
						e.printStackTrace();
					}
				
			};
		};
		Runnable r2 = new Runnable(){	 
			@Override
			public void run() {
				try {
					Model model1 = ModelFactory.createDefaultModel().read(IOUtils.toInputStream(EXAMPLE_RDF_FRAMGMENT_2, "UTF-8"), HelioConfiguration.DEFAULT_BASE_URI, "TURTLE");
					HelioConfiguration.HELIO_CACHE.addGraph("http://test.com/good2", model1);
					Thread.currentThread().setName("Storing fragment 2");
					}catch(Exception e) {
						e.printStackTrace();
					}
			};
		};
		Runnable r3 = new Runnable(){	 
			@Override
			public void run() {
				HelioConfiguration.HELIO_CACHE.getGraph("http://test.com/good1");
				Thread.currentThread().setName("Reading fragment 1");
		
					String query = "SELECT ?type { ?s a ?type .}";	
					HelioConfiguration.HELIO_CACHE.solveTupleQuery(query, SparqlResultsFormat.JSON);
				
			};
		};
		Runnable r4 = new Runnable(){	 
			@Override
			public void run() {
				HelioConfiguration.HELIO_CACHE.getGraph("http://test.com/good2");
				Thread.currentThread().setName("Reading fragment 2");
			
					String query = "SELECT ?type { ?s a ?type .}";	
					HelioConfiguration.HELIO_CACHE.solveTupleQuery(query, SparqlResultsFormat.JSON);
			
			};
		};
		Runnable r5 = new Runnable(){	 
			@Override
			public void run() {
				String[] iris = new String[] {"http://test.com/good1", "http://test.com/good2"};
				HelioConfiguration.HELIO_CACHE.getGraphs(iris);
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
			pool.shutdown();
			if(WAIT_FOR_SYNCHRONOUS_TRANSFORMATION_TO_FINISH)
				pool.awaitTermination(99, TimeUnit.DAYS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		
			String query = "SELECT ?type { ?s a ?type .}";	
			String  input  = HelioConfiguration.HELIO_CACHE.solveTupleQuery(query, SparqlResultsFormat.JSON);
			Assert.assertFalse(input.isEmpty());
		
		HelioConfiguration.HELIO_CACHE.deleteGraphs();
	}
	
	
	@Test
	public void multipleQueries2() throws InterruptedException, ExecutionException {
		HelioConfiguration.HELIO_CACHE.deleteGraphs();
		//HelioConfiguration.HELIO_CACHE.addGraph("http://test.com/good1", EXAMPLE_RDF_FRAMGMENT_1, RDFFormat.TURTLE);
		Runnable r1 = new Runnable(){	 
			@Override
			public void run() {
				try {
					Model model1 = ModelFactory.createDefaultModel().read(IOUtils.toInputStream(EXAMPLE_RDF_FRAMGMENT_1, "UTF-8"), HelioConfiguration.DEFAULT_BASE_URI, "TURTLE");
					HelioConfiguration.HELIO_CACHE.addGraph("http://test.com/good1", model1);
					Thread.currentThread().setName("Storing fragment 1");
					}catch(Exception e) {
						e.printStackTrace();
					}
			};
		};
		Runnable r2 = new Runnable(){	 
			@Override
			public void run() {
				try {
					Model model1 = ModelFactory.createDefaultModel().read(IOUtils.toInputStream(EXAMPLE_RDF_FRAMGMENT_2, "UTF-8"), HelioConfiguration.DEFAULT_BASE_URI, "TURTLE");
					HelioConfiguration.HELIO_CACHE.addGraph("http://test.com/good2", model1);
					Thread.currentThread().setName("Storing fragment 2");
					}catch(Exception e) {
						e.printStackTrace();
					}
			};
		};
		
		Runnable r4 = new Runnable(){	 
			@Override
			public void run() {
				
				Thread.currentThread().setName("Storing fragment 2");
			};
		};
		
		final ForkJoinPool pool = ForkJoinPool.commonPool();
		pool.execute(r1);
		pool.execute(r2);
		pool.execute(r4);
		
		
		try {
			if(WAIT_FOR_SYNCHRONOUS_TRANSFORMATION_TO_FINISH)
				pool.awaitTermination(99, TimeUnit.DAYS);
				pool.shutdownNow();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
	
			String query = "SELECT DISTINCT ?s { ?s ?p ?type .}";	
			String  input  = HelioConfiguration.HELIO_CACHE.solveTupleQuery(query, SparqlResultsFormat.JSON);
			Assert.assertFalse(input.isEmpty());
	
		HelioConfiguration.HELIO_CACHE.deleteGraphs();
	}
	
	@Test
	public void stressTest() throws IOException {
		HelioConfiguration.HELIO_CACHE.deleteGraphs();
		int index = 0;
		long max = 100;
		long startTime = System.nanoTime();
		while(index < max) {
			
			HelioConfiguration.HELIO_CACHE.deleteGraphs();
			Model model1 = ModelFactory.createDefaultModel().read(IOUtils.toInputStream(EXAMPLE_RDF_FRAMGMENT_1, "UTF-8"), HelioConfiguration.DEFAULT_BASE_URI, "TURTLE");
			HelioConfiguration.HELIO_CACHE.addGraph("http://test.com/good1", model1);
			Model model2 = ModelFactory.createDefaultModel().read(IOUtils.toInputStream(EXAMPLE_RDF_FRAMGMENT_2, "UTF-8"), HelioConfiguration.DEFAULT_BASE_URI, "TURTLE");
			HelioConfiguration.HELIO_CACHE.addGraph("http://test.com/good1", model2);
			
			
			Assert.assertFalse(HelioConfiguration.HELIO_CACHE.getGraphs().isEmpty());
			
			String query = "SELECT DISTINCT ?s { ?s ?p ?type .}";	
			String  input  = HelioConfiguration.HELIO_CACHE.solveTupleQuery(query, SparqlResultsFormat.JSON);
			Assert.assertFalse(input.isEmpty());
		
			index++;
		}
		long elapsedTime = System.nanoTime() - startTime;   
		System.out.println("1000 times executed  oeprations: Delete, Add fragment 1, Add fragment 2, perform query. Execution in millis: "  + elapsedTime/1000000);
		HelioConfiguration.HELIO_CACHE.deleteGraphs();
	}
}
