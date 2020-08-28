package helio.materialiser.engine.mappings;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.vocabulary.RDF;
import org.junit.Assert;
import org.junit.Test;

import helio.framework.exceptions.MalformedMappingException;
import helio.framework.materialiser.MappingTranslator;
import helio.framework.materialiser.mappings.HelioMaterialiserMapping;
import helio.materialiser.HelioMaterialiser;
import helio.materialiser.configuration.HelioConfiguration;
import helio.materialiser.mappings.JsonTranslator;
import helio.materialiser.test.utils.TestUtils;

public class LinkingTest {

	@Test
	public void test1() throws IOException, MalformedMappingException, InterruptedException {
		HelioConfiguration.HELIO_CACHE.deleteGraphs();
		
		String mappingFile = "./src/test/resources/linking-tests/test-linking-1-mapping.json";
		String expectedFile = "./src/test/resources/linking-tests/test-linking-1-expected.ttl";
		HelioConfiguration.HELIO_CACHE.deleteGraphs();
		Model expected = TestUtils.readModel(expectedFile);
		Model generated = TestUtils.generateRDFSynchronously(mappingFile);
		
		Assert.assertTrue(TestUtils.compareModels(generated, expected));
		HelioConfiguration.HELIO_CACHE.deleteGraphs();
	}
	
	@Test
	public void test2() throws IOException, MalformedMappingException, InterruptedException {
		HelioConfiguration.HELIO_CACHE.deleteGraphs();
		
		String mappingFile = "./src/test/resources/linking-tests/test-linking-2-mapping.json";
		String expectedFile = "./src/test/resources/linking-tests/test-linking-2-expected.ttl";
		HelioConfiguration.HELIO_CACHE.deleteGraphs();
		Model expected = TestUtils.readModel(expectedFile);
		Model generated = TestUtils.generateRDFSynchronously(mappingFile);
		
		Assert.assertTrue(TestUtils.compareModels(generated, expected));
		HelioConfiguration.HELIO_CACHE.deleteGraphs();
	}
	
	@Test
	public void test3() throws IOException, MalformedMappingException, InterruptedException {
		Thread.sleep(1000);
		HelioConfiguration.HELIO_CACHE.deleteGraphs();
		HelioConfiguration.EVALUATOR.closeH2Cache();
		String mappingStr1 = TestUtils.readFile("./src/test/resources/bimr-tests/helio-1-mapping.json");
		String mappingStr2 = TestUtils.readFile("./src/test/resources/bimr-tests/helio-2-mapping.json");
		String mappingStr3 = TestUtils.readFile("./src/test/resources/linking-tests/test-async-linking-1-mapping.json");
		
		MappingTranslator translator = new JsonTranslator();
		HelioMaterialiserMapping mapping = translator.translate(mappingStr1);
		mapping.merge(translator.translate(mappingStr2));
		mapping.merge(translator.translate(mappingStr3));
		HelioMaterialiser helio = new HelioMaterialiser(mapping);
		helio.updateSynchronousSources();
		
		Model generated = ModelFactory.createDefaultModel();
		Set<String> irisFound = new HashSet<>();
		while(irisFound.size()< 3) {
			generated = helio.getRDF();
			Iterator<Statement> iterable = generated.listStatements(null, RDF.type, ResourceFactory.createResource("http://www.exmaple.com/test#AsyncResource"));
			while(iterable.hasNext()) {
				String uri = iterable.next().asTriple().getSubject().getURI();
				irisFound.add(uri);
			}
		}
		generated = helio.getRDF();
		Boolean correct = true;
		for(String iri:irisFound)
			correct &= generated.contains(ResourceFactory.createResource(iri), ResourceFactory.createProperty("http://www.w3.org/2002/07/owl#sameAs"), ResourceFactory.createResource("https://www.data.bimerr.occupancy.es/resource/sync/Building_1"));		
		
		Assert.assertTrue(true);
	}
	
	
	
	
}
