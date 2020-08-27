package helio.materialiser.run;

import java.io.BufferedReader;
import java.io.FileReader;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.repository.sparql.config.SPARQLRepositoryConfig;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;

import helio.framework.exceptions.MalformedMappingException;
import helio.framework.materialiser.MappingTranslator;
import helio.framework.materialiser.mappings.HelioMaterialiserMapping;
import helio.materialiser.HelioMaterialiser;
import helio.materialiser.configuration.HelioConfiguration;
import helio.materialiser.mappings.AutomaticTranslator;

public class Test {

	public static void main(String[] args) throws MalformedMappingException   {
		System.out.println(SPARQLRepositoryConfig.NAMESPACE);
		System.out.println(SPARQLRepositoryConfig.QUERY_ENDPOINT);
		System.out.println(SPARQLRepositoryConfig.UPDATE_ENDPOINT);
		
		//Repository repo = new VirtuosoRepository("jdbc:virtuoso://localhost:8890", "dba", "dba");
		//Repository repo = new SPARQLRepository("http://localhost:7200/repositories/discovery/statements");
		//HelioMaterialiser.HELIO_CACHE.changeRepository(repo);
		//HelioMaterialiser.HELIO_CACHE.changeSailRepository(new SailRepository(new MemoryStore(new File("./rdf4j-test"))));
		String content = readFile("./config.ttl");
		HelioMaterialiser.HELIO_CACHE.configureRepository(content);
		/*LinkedHashModel rdf4JModel = new LinkedHashModel();
		(new SPARQLRepositoryConfig("\"jdbc:virtuoso://localhost:8890\"","\"jdbc:virtuoso://localhost:8890\"")).export(rdf4JModel);
		Rio.write(rdf4JModel, System.out, RDFFormat.TURTLE);*/
		
		HelioConfiguration.THREADS_INJECTING_DATA=20;
		HelioConfiguration.THREADS_HANDLING_DATA=100;
		
		Model rdf = generateRDFSynchronously("./src/test/resources/bimr-tests/helio-3-mapping.json");
		rdf.write(System.out, "TTL");
		/*
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
