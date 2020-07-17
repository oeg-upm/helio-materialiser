package helio.materialiser.run;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PipedInputStream;

import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.manager.RemoteRepositoryManager;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sparql.SPARQLRepository;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.junit.Assert;

import helio.framework.exceptions.MalformedMappingException;
import helio.framework.mapping.Mapping;
import helio.framework.materialiser.mappings.HelioMaterialiserMapping;
import helio.framework.objects.SparqlResultsFormat;
import helio.materialiser.HelioMaterialiser;
import helio.materialiser.mappings.JsonTranslator;

public class Test {

	public static void main(String[] args)   {
		
		Repository repo = new SPARQLRepository("http://localhost:7200/repositories/discovery/statements");
		HelioMaterialiser.HELIO_CACHE.changeSailRepository(repo);
		//HelioMaterialiser.HELIO_CACHE.changeSailRepository(new SailRepository(new MemoryStore(new File("./rdf4j-test"))));
		
		HelioMaterialiserMapping mapping = new HelioMaterialiserMapping();
		JsonTranslator translator = new JsonTranslator();
		
		File mappingFolder = new File("./mappings");
		File[] files = mappingFolder.listFiles();
		for(int index=0; index <files.length; index++) {
			File file = files[index];
			String mappingStr = readFile(file.getAbsolutePath());
			try {
				if(translator.isCompatible(mappingStr)) {	
					HelioMaterialiserMapping mappingAux = translator.translate(mappingStr);
					mapping.merge(mappingAux);
				}
			}catch(Exception e) {
				System.out.println("check the file: "+file);
			}
		}
		System.out.println("Data sources: "+mapping.getDatasources());
		System.out.println("Rules: "+mapping.getRuleSets());
		HelioMaterialiser helio = new HelioMaterialiser(mapping);
		long startTime = System.nanoTime();
		helio.updateSynchronousSources();
		helio.close();

		long timeElapsed = System.nanoTime() - startTime;
		System.out.println("Execution time in nanoseconds  : " + timeElapsed);
		System.out.println("Execution time in milliseconds : " +	timeElapsed / 1000000);
		
	}
	
	
	 public static String readFile(String fileName) {
		 StringBuilder data = new StringBuilder();
			// 1. Read the file
			try {
				FileReader file = new FileReader(fileName);
				BufferedReader bf = new BufferedReader(file);
				// 2. Accumulate its lines in the data var
				bf.lines().forEach( line -> data.append(line).append("\n"));
				bf.close();
				file.close();
				// TODO: Opening and clossing im not sure is the best option 
			}catch(Exception e) {
				e.printStackTrace();
			} 
			return data.toString();
	 }


}
