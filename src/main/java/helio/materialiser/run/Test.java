package helio.materialiser.run;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.concurrent.TimeUnit;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;

import com.google.common.base.Stopwatch;

import helio.framework.exceptions.MalformedMappingException;
import helio.framework.materialiser.MappingTranslator;
import helio.framework.materialiser.mappings.HelioMaterialiserMapping;
import helio.materialiser.HelioMaterialiser;
import helio.materialiser.HelioUtils;
import helio.materialiser.configuration.HelioConfiguration;
import helio.materialiser.mappings.AutomaticTranslator;
import helio.materialiser.mappings.JsonTranslator;
import helio.materialiser.mappings.RMLTranslator;

public class Test {

	public static void main(String[] args) throws MalformedMappingException   {
		
		Stopwatch stopwatch = Stopwatch.createStarted();
		String mappingContent = HelioUtils.readFile("/Users/andrea/Desktop/test/rml-mapping.ttl.txt");
		//String mappingContent2 = HelioUtils.readFile("/Users/cimmino/Desktop/salvatest/mapping.json");

		MappingTranslator translator = new AutomaticTranslator();
		HelioMaterialiserMapping mapping = translator.translate(mappingContent);
		HelioConfiguration.HELIO_CACHE.deleteGraphs();
		HelioMaterialiser helio = new HelioMaterialiser(mapping);
		helio.updateSynchronousSources();
		helio.getRDF().write(System.out, "TTL");
		helio.close();
		
		stopwatch.stop(); // optional
		System.out.println("Time elapsed: "+ stopwatch.elapsed(TimeUnit.MILLISECONDS));
		
		//String content = readFile("./config.ttl");
		//HelioConfiguration.HELIO_CACHE.configureRepository(content);
		//HelioConfiguration.readConfigurationFile("/Users/cimmino/Desktop/test-run/helio-configuration.json");
		//Model rdf = generateRDFSynchronously("./src/test/resources/bimr-tests/helio-3-mapping.json");
		//rdf.write(System.out, "TTL");
		/*
		HelioMaterialiserMapping mapping = new HelioMaterialiserMapping();
		JsonTranslator translator = new JsonTranslator();
		
		File mappingFolder = new File("./PruebaHelio");
		File[] files = mappingFolder.listFiles();
		for(int index=0; index <files.length; index++) {
			File file = files[index];
			String mappingStr = readFile(file.getAbsolutePath());
			try {
				if(translator.isCompatible(mappingStr)) {	
					HelioMaterialiserMapping mappingAux = translator.translate(mappingStr);
					mapping.merge(mappingAux);
				}else {
					System.out.println("check the file: "+file);
				}
			}catch(Exception e) {
				System.out.println("check the file: "+file);
			}
		}
		System.out.println("Data sources: "+mapping.getDatasources());
		System.out.println("Rules: "+mapping.getRuleSets());
		for(int index=0; index<20; index++) {
			//System.out.println("Initial memory: "+(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory())/(1024 * 1024));
			HelioMaterialiser helio = new HelioMaterialiser(mapping);
			long startTime = System.nanoTime();
			helio.updateSynchronousSources();
			long timeElapsed = System.nanoTime() - startTime;
			if(helio.getRDF().size()<1)
				throw new IllegalArgumentException();
			//System.out.println("Execution time in nanoseconds  : " + timeElapsed);
			System.out.println("Execution time in milliseconds : " +	timeElapsed / 1000000);
			helio.close();
			//System.out.println("Initial memory: "+(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory())/(1024 * 1024));
		}
		*/
	}
	
	public static Model generateRDFSynchronously(String mappingFile) throws MalformedMappingException {
		String mappingContent = readFile(mappingFile);
		Model model = ModelFactory.createDefaultModel();

		MappingTranslator translator = new AutomaticTranslator();
		HelioMaterialiserMapping mapping = translator.translate(mappingContent);
			
		HelioMaterialiser helio = new HelioMaterialiser(mapping);
		helio.updateSynchronousSources();
		model = helio.getRDF();
		helio.close();
		
		return model;
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
