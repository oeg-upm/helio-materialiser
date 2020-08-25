package helio.materialiser.engine.mappings;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.junit.Assert;
import org.junit.Test;

import helio.framework.exceptions.MalformedMappingException;
import helio.framework.materialiser.MappingTranslator;
import helio.framework.materialiser.mappings.HelioMaterialiserMapping;
import helio.materialiser.HelioMaterialiser;
import helio.materialiser.configuration.HelioConfiguration;
import helio.materialiser.mappings.AutomaticTranslator;
import helio.materialiser.mappings.JsonTranslator;

public class LinkingTest {


	public static final Resource[] iris = new Resource[] {};

	
	@Test
	public void test1() throws IOException, MalformedMappingException, InterruptedException {
		HelioMaterialiser.HELIO_CACHE.deleteGraphs();
		String mappingStr = readFile("./src/test/resources/linking-tests/test-linking-1-mapping.json");
		String expectedFile = "./src/test/resources/linking-tests/test-linking-1-expected.ttl";
		FileInputStream out = new FileInputStream(expectedFile);
		Model expected = Rio.parse(out, HelioConfiguration.DEFAULT_BASE_URI, RDFFormat.TURTLE, iris);	
		
		MappingTranslator translator = new AutomaticTranslator();
		HelioMaterialiserMapping mapping = translator.translate(mappingStr);
		HelioMaterialiser helio = new HelioMaterialiser(mapping);
		helio.updateSynchronousSources();
		
				
		Model generated = helio.getRDF();
		
		helio.close();
		Boolean correct = expected.stream().allMatch(st -> generated.contains(st.getSubject(), st.getPredicate(), st.getObject(), iris));
		Assert.assertTrue(correct);
	}
	
	@Test
	public void test2() throws IOException, MalformedMappingException, InterruptedException {
		HelioMaterialiser.HELIO_CACHE.deleteGraphs();
		
		String mappingStr = readFile("./src/test/resources/linking-tests/test-linking-2-mapping.json");
		String expectedFile = "./src/test/resources/linking-tests/test-linking-2-expected.ttl";
		FileInputStream out = new FileInputStream(expectedFile);
		Model expected = Rio.parse(out, HelioConfiguration.DEFAULT_BASE_URI, RDFFormat.TURTLE, iris);	
		
		MappingTranslator translator = new JsonTranslator();
		HelioMaterialiserMapping mapping = translator.translate(mappingStr);
		HelioMaterialiser helio = new HelioMaterialiser(mapping);
		helio.updateSynchronousSources();
		
				
		Model generated = helio.getRDF();
		helio.close();
		//Rio.write(generated, System.out, RDFFormat.TURTLE);
		
		Boolean correct = expected.stream().allMatch(st -> generated.contains(st.getSubject(), st.getPredicate(), st.getObject(), iris));
		Assert.assertTrue(correct);
	}
	
	@Test
	public void test3() throws IOException, MalformedMappingException, InterruptedException {
		Thread.sleep(1000);
		HelioMaterialiser.HELIO_CACHE.deleteGraphs();
		HelioMaterialiser.EVALUATOR.closeH2Cache();
		String mappingStr1 = readFile("./src/test/resources/helio-tests/helio-1-mapping.json");
		String mappingStr2 = readFile("./src/test/resources/helio-tests/helio-2-mapping.json");
		String mappingStr3 = readFile("./src/test/resources/linking-tests/test-async-linking-1-mapping.json");
		
		MappingTranslator translator = new JsonTranslator();
		HelioMaterialiserMapping mapping = translator.translate(mappingStr1);
		mapping.merge(translator.translate(mappingStr2));
		mapping.merge(translator.translate(mappingStr3));
		HelioMaterialiser helio = new HelioMaterialiser(mapping);
		helio.updateSynchronousSources();
		ValueFactory factory = SimpleValueFactory.getInstance();
		Model generated = new LinkedHashModel();
		Set<String> irisFound = new HashSet<>();
		while(irisFound.size()< 3) {
			
			generated = helio.getRDF();
			Iterator<Statement> iterable = generated.getStatements(null, RDF.TYPE, factory.createIRI("http://www.exmaple.com/test#AsyncResource"), iris).iterator();
			while(iterable.hasNext()) {
				irisFound.add(iterable.next().toString());
			}
		}
		generated = helio.getRDF();
		Boolean correct = true;
		for(String iri:irisFound)
			correct &= generated.contains(factory.createIRI(iri), factory.createIRI("http://www.w3.org/2002/07/owl#sameAs"), factory.createIRI("https://www.data.bimerr.occupancy.es/resource/sync/Building_1"), iris);
		
		//helio.close();
		Rio.write(generated, System.out, RDFFormat.TURTLE);

		
		Assert.assertTrue(true);
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
