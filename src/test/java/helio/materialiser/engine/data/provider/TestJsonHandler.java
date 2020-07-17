package helio.materialiser.engine.data.provider;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.Queue;
import org.junit.Test;

import helio.materialiser.HelioMaterialiser;
import helio.materialiser.data.handlers.JsonHandler;
import helio.materialiser.data.providers.InMemoryProvider;

public class TestJsonHandler {

	private static final String JSON_DOCUMENT = "{\n" + 
			"    \"book\": \n" + 
			"    [\n" + 
			"        {\n" + 
			"            \"title\": \"Beginning JSON\",\n" + 
			"            \"author\": \"Ben Smith\",\n" + 
			"            \"price\": 49.99\n" + 
			"        },\n" + 
			" \n" + 
			"        {\n" + 
			"            \"title\": \"JSON at Work\",\n" + 
			"            \"author\": \"Tom Marrs\",\n" + 
			"            \"price\": 29.99\n" + 
			"        },\n" + 
			" \n" + 
			"        {\n" + 
			"            \"title\": \"Learn JSON in a DAY\",\n" + 
			"            \"author\": \"Acodemy\",\n" + 
			"            \"price\": 8.99\n" + 
			"        },\n" + 
			" \n" + 
			"        {\n" + 
			"            \"title\": \"JSON: Questions and Answers\",\n" + 
			"            \"author\": \"George Duckett\",\n" + 
			"            \"price\": 6.00\n" + 
			"        },\n" + 
			"        {\n" + 
			"            \"author\": \"George Duckett 2\",\n" + 
			"            \"price\": 6.00\n" + 
			"        }\n" + 
			"    ],\n" + 
			" \n" + 
			"    \"price range\": \n" + 
			"    {\n" + 
			"        \"cheap\": 10.00,\n" + 
			"        \"medium\": 20.00\n" + 
			"    }\n" + 
			"}";
	
	
	
	@Test
	public void test() throws IOException {
		HelioMaterialiser.HELIO_CACHE.deleteGraphs();
		PipedOutputStream output = new PipedOutputStream();
		PipedInputStream input = new PipedInputStream(output);
		
		output.write(JSON_DOCUMENT.getBytes());
		output.close();
		InMemoryProvider memoryProvider = new InMemoryProvider(input);
		JsonHandler jsonHandler = new JsonHandler("$.book[*]");
		Queue<String> results = jsonHandler.splitData(memoryProvider.getData());
	

		Runnable r1 = new Runnable(){	 
			public void run() {
				String fragment = results.poll();
				if(fragment!=null) {
					String value = jsonHandler.filter("$.title", fragment);
					Thread.currentThread().setName("Storing fragment 1 -> "+value);
					
				}
			};
		};
		Runnable r2 = new Runnable(){	 
			public void run() {
				String fragment = results.poll();
				if(fragment!=null) {
					String value = jsonHandler.filter("$.title", fragment);
					Thread.currentThread().setName("Storing fragment 2 -> "+value);
					
				}
			};
		};
		Runnable r3 = new Runnable(){	 
			public void run() {
				String fragment = results.poll();
				if(fragment!=null) {
					String value = jsonHandler.filter("$.title", fragment);
					Thread.currentThread().setName("Storing fragment 3 -> "+value);
					
				}
			};
		};
		Runnable r4 = new Runnable(){	 
			public void run() {
				String fragment = results.poll();
				if(fragment!=null) {
					String value = jsonHandler.filter("$.title", fragment);
					Thread.currentThread().setName("Storing fragment 4 -> "+value);
					
				}
			};
		};
		Runnable r5 = new Runnable(){	 
			public void run() {
				String fragment = results.poll();
				if(fragment!=null) {
					String value = jsonHandler.filter("$.title", fragment);
					
					Thread.currentThread().setName("Storing fragment 5 -> "+value);
					
				}
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
		
		while(t1.isAlive() || t2.isAlive() || t3.isAlive() || t4.isAlive() || t5.isAlive()) {
			
		}
	
	}

}
