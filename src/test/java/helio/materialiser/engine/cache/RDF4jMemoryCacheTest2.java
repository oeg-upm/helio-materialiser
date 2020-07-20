package helio.materialiser.engine.cache;

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

import helio.framework.objects.SparqlResultsFormat;
import helio.materialiser.HelioMaterialiser;
import helio.materialiser.configuration.HelioConfiguration;

public class RDF4jMemoryCacheTest2 {

	private static Logger logger = LogManager.getLogger(RDF4jMemoryCacheTest2.class);

	private static final String EXAMPLE_RDF_FRAMGMENT_1 = "<http://www.w3.org/People/EM/contact#FM> <http://www.w3.org/2000/10/swap/pim/contact#fullName> \"Frank Miller\";\n" + 
			"	<http://www.w3.org/2000/10/swap/pim/contact#personalTitle> \"Mr.\" ;\n" + 
			"	<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2000/10/swap/pim/contact#Person> .";
	
	private static final String EXAMPLE_RDF_FRAMGMENT_2 = "<http://www.w3.org/People/EM/contact#me> <http://www.w3.org/2000/10/swap/pim/contact#fullName> \"Eric Miller\";\n" + 
			"	<http://www.w3.org/2000/10/swap/pim/contact#mailbox> <mailto:e.miller123(at)example> ;\n" + 
			"	<http://www.w3.org/2000/10/swap/pim/contact#personalTitle> \"Dr.\" ;\n" + 
			"	<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2000/10/swap/pim/contact#Person> .";
	
	@Test
	public void testReadWriteOneGraph() {
		
		HelioMaterialiser.HELIO_CACHE.addGraph("http://test.com/good", EXAMPLE_RDF_FRAMGMENT_1, RDFFormat.TURTLE);
		Model model = HelioMaterialiser.HELIO_CACHE.getGraph("http://test.com/good");
		Assert.assertFalse(model.isEmpty());
	}
	
	
	@Test
	public void testWriteMultipleGraphsReadALL() {
		HelioMaterialiser.HELIO_CACHE.addGraph("http://test.com/good1", EXAMPLE_RDF_FRAMGMENT_1, RDFFormat.TURTLE);
		HelioMaterialiser.HELIO_CACHE.addGraph("http://test.com/good2", EXAMPLE_RDF_FRAMGMENT_2, RDFFormat.TURTLE);
		Model model = HelioMaterialiser.HELIO_CACHE.getGraphs();
		Assert.assertFalse(model.isEmpty());
	}
	
	
	@Test
	public void testReadWriteMultipleGraphs() {
		HelioMaterialiser.HELIO_CACHE.addGraph("http://test.com/good1", EXAMPLE_RDF_FRAMGMENT_1, RDFFormat.TURTLE);
		HelioMaterialiser.HELIO_CACHE.addGraph("http://test.com/good2", EXAMPLE_RDF_FRAMGMENT_2, RDFFormat.TURTLE);
		String[] iris = new String[] {"http://test.com/good1", "http://test.com/good2"};
		Model model = HelioMaterialiser.HELIO_CACHE.getGraphs(iris);
		Assert.assertFalse(model.isEmpty());
	}
	
	@Test
	public void testReadWriteMultipleGraphsMultipleThreads() {
		

		Runnable r1 = new Runnable(){	 
			@Override
			public void run() {
				HelioMaterialiser.HELIO_CACHE.addGraph("http://test.com/good1", EXAMPLE_RDF_FRAMGMENT_1, RDFFormat.TURTLE);
				Thread.currentThread().setName("Storing fragment 1");
			};
		};
		Runnable r2 = new Runnable(){	 
			@Override
			public void run() {
				HelioMaterialiser.HELIO_CACHE.addGraph("http://test.com/good2", EXAMPLE_RDF_FRAMGMENT_2, RDFFormat.TURTLE);
				Thread.currentThread().setName("Storing fragment 2");
			};
		};
		Runnable r3 = new Runnable(){	 
			@Override
			public void run() {
				HelioMaterialiser.HELIO_CACHE.getGraph("http://test.com/good1");
				Thread.currentThread().setName("Reading fragment 1");
			};
		};
		Runnable r4 = new Runnable(){	 
			@Override
			public void run() {
				HelioMaterialiser.HELIO_CACHE.getGraph("http://test.com/good2");
				Thread.currentThread().setName("Reading fragment 2");
			};
		};
		Runnable r5 = new Runnable(){	 
			@Override
			public void run() {
				String[] iris = new String[] {"http://test.com/good1", "http://test.com/good2"};
				HelioMaterialiser.HELIO_CACHE.getGraphs(iris);
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
		Model model = HelioMaterialiser.HELIO_CACHE.getGraphs(iris);
		Assert.assertFalse(model.isEmpty());
	}
	
	@Test
	public void query() throws IOException {
		HelioMaterialiser.HELIO_CACHE.addGraph("http://test.com/good1", EXAMPLE_RDF_FRAMGMENT_1, RDFFormat.TURTLE);
		HelioMaterialiser.HELIO_CACHE.addGraph("http://test.com/good2", EXAMPLE_RDF_FRAMGMENT_2, RDFFormat.TURTLE);
		
		String query = "SELECT ?type { ?s a ?type .}";	
		PipedInputStream  input  = HelioMaterialiser.HELIO_CACHE.solveTupleQuery(query, SparqlResultsFormat.JSON);
		int data = input.read();
		StringBuilder builder = new StringBuilder();
		while(data != -1){
			builder.append((char) data);
            data = input.read();
        }
		input.close();
		Assert.assertFalse(builder.toString().isEmpty());
	}
	
	
	@Test
	public void multipleQueries() throws InterruptedException, ExecutionException {
		Runnable r1 = new Runnable(){	 
			@Override
			public void run() {
				
				HelioMaterialiser.HELIO_CACHE.addGraph("http://test.com/good1", EXAMPLE_RDF_FRAMGMENT_1, RDFFormat.TURTLE);
				Thread.currentThread().setName("Storing fragment 1");
			};
		};
		Runnable r2 = new Runnable(){	 
			@Override
			public void run() {
				HelioMaterialiser.HELIO_CACHE.addGraph("http://test.com/good2", EXAMPLE_RDF_FRAMGMENT_2, RDFFormat.TURTLE);
				Thread.currentThread().setName("Storing fragment 2");
			};
		};
		Runnable r3 = new Runnable(){	 
			@Override
			public void run() {
				HelioMaterialiser.HELIO_CACHE.getGraph("http://test.com/good1");
				Thread.currentThread().setName("Reading fragment 1");
				try {
					String query = "SELECT ?type { ?s a ?type .}";	
					PipedInputStream  input  = HelioMaterialiser.HELIO_CACHE.solveTupleQuery(query, SparqlResultsFormat.JSON);
					int data = input.read();
					
					StringBuilder builder = new StringBuilder();
					while(data != -1){
						builder.append((char) data);
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
				HelioMaterialiser.HELIO_CACHE.getGraph("http://test.com/good2");
				Thread.currentThread().setName("Reading fragment 2");
				try {
					String query = "SELECT ?type { ?s a ?type .}";	
					PipedInputStream  input  = HelioMaterialiser.HELIO_CACHE.solveTupleQuery(query, SparqlResultsFormat.JSON);
					int data = input.read();
					
					StringBuilder builder = new StringBuilder();
					while(data != -1){
						builder.append((char) data);
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
				HelioMaterialiser.HELIO_CACHE.getGraphs(iris);
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

		try {

			String query = "SELECT ?type { ?s a ?type .}";	
			PipedInputStream  input  = HelioMaterialiser.HELIO_CACHE.solveTupleQuery(query, SparqlResultsFormat.JSON);
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
	}
	
	
	@Test
	public void multipleQueries2() throws InterruptedException, ExecutionException {
		//HelioMaterialiser.HELIO_CACHE.addGraph("http://test.com/good1", EXAMPLE_RDF_FRAMGMENT_1, RDFFormat.TURTLE);
		Runnable r1 = new Runnable(){	 
			@Override
			public void run() {
				HelioMaterialiser.HELIO_CACHE.addGraph("http://test.com/good1", EXAMPLE_RDF_FRAMGMENT_1, RDFFormat.TURTLE);
				Thread.currentThread().setName("Storing fragment 1");
			};
		};
		Runnable r2 = new Runnable(){	 
			@Override
			public void run() {
				HelioMaterialiser.HELIO_CACHE.addGraph("http://test.com/good1", EXAMPLE_RDF_FRAMGMENT_1, RDFFormat.TURTLE);
				Thread.currentThread().setName("Storing fragment 2");
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
			if(HelioConfiguration.WAIT_FOR_SYNCHRONOUS_TRANSFORMATION_TO_FINISH)
				pool.awaitTermination(99, TimeUnit.DAYS);
				pool.shutdownNow();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		try {

			String query = "SELECT DISTINCT ?s { ?s ?p ?type .}";	
			PipedInputStream  input  = HelioMaterialiser.HELIO_CACHE.solveTupleQuery(query, SparqlResultsFormat.JSON);
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
	}
	
	@Test
	public void stressTest() {
		int index = 0;
		long max = 1000;
		long startTime = System.nanoTime();
		while(index < max) {
			
			HelioMaterialiser.HELIO_CACHE.deleteGraphs();
			HelioMaterialiser.HELIO_CACHE.addGraph("http://test.com/good1", EXAMPLE_RDF_FRAMGMENT_1, RDFFormat.TURTLE);
			HelioMaterialiser.HELIO_CACHE.addGraph("http://test.com/good1", EXAMPLE_RDF_FRAMGMENT_2, RDFFormat.TURTLE);
			
			Assert.assertFalse(HelioMaterialiser.HELIO_CACHE.getGraphs().isEmpty());
			
			try {

				String query = "SELECT DISTINCT ?s { ?s ?p ?type .}";	
				PipedInputStream  input  = HelioMaterialiser.HELIO_CACHE.solveTupleQuery(query, SparqlResultsFormat.JSON);
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
