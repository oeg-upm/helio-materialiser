package helio.materialiser.engine.mappings;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.junit.Assert;
import org.junit.Test;

import helio.framework.exceptions.MalformedMappingException;
import helio.framework.materialiser.MappingTranslator;
import helio.framework.materialiser.mappings.HelioMaterialiserMapping;
import helio.materialiser.HelioMaterialiser;
import helio.materialiser.configuration.HelioConfiguration;
import helio.materialiser.evaluator.H2Evaluator;
import helio.materialiser.mappings.AutomaticTranslator;

public class LinkingTest {


	public static final Resource[] iris = new Resource[] {};

	
	@Test
	public void test1() throws IOException, MalformedMappingException, InterruptedException {
		String mappingStr = readFile("./src/test/resources/linking-tests/test-linking-1-mapping.json");
		String expectedFile = "./src/test/resources/linking-tests/test-linking-1-expected.ttl";
		FileInputStream out = new FileInputStream(expectedFile);
		Model expected = Rio.parse(out, HelioConfiguration.DEFAULT_BASE_URI, RDFFormat.TURTLE, iris);	
		
		HelioMaterialiser.HELIO_CACHE.deleteGraphs();
		MappingTranslator translator = new AutomaticTranslator();
		HelioMaterialiserMapping mapping = translator.translate(mappingStr);
		
		HelioMaterialiser helio = new HelioMaterialiser(mapping);
		helio.updateSynchronousSources();
		H2Evaluator evaluator = new H2Evaluator();
		evaluator.linkData();
				
		Model generated = helio.getRDF();

		helio.close();
		Boolean correct = expected.stream().allMatch(st -> generated.contains(st.getSubject(), st.getPredicate(), st.getObject(), iris));
		Assert.assertTrue(correct);
		Thread.sleep(1000);
	}
	
	@Test
	public void test2() throws IOException, MalformedMappingException, InterruptedException {
		Thread.sleep(1000);
		String mappingStr = readFile("./src/test/resources/linking-tests/test-linking-2-mapping.json");
		String expectedFile = "./src/test/resources/linking-tests/test-linking-2-expected.ttl";
		FileInputStream out = new FileInputStream(expectedFile);
		Model expected = Rio.parse(out, HelioConfiguration.DEFAULT_BASE_URI, RDFFormat.TURTLE, iris);	
		
		HelioMaterialiser.HELIO_CACHE.deleteGraphs();
		MappingTranslator translator = new AutomaticTranslator();
		HelioMaterialiserMapping mapping = translator.translate(mappingStr);
		HelioMaterialiser helio = new HelioMaterialiser(mapping);
		helio.updateSynchronousSources();
		H2Evaluator evaluator = new H2Evaluator();
		evaluator.linkData();
				
		Model generated = helio.getRDF();
		helio.close();
		Rio.write(generated, System.out, RDFFormat.TURTLE);
		
		Boolean correct = expected.stream().allMatch(st -> generated.contains(st.getSubject(), st.getPredicate(), st.getObject(), iris));
		Assert.assertTrue(correct);
	}
	
	
	private static String readFile(String fileName) {
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
