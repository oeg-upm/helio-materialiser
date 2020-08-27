package helio.materialiser.engine;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
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

public class BimerTest {
	
	


	
	@Test
	public void test1() throws IOException, MalformedMappingException, InterruptedException {
		HelioMaterialiser.HELIO_CACHE.deleteGraphs();
		Model expected = TestUtils.readModel("./src/test/resources/bimr-tests/helio-1-expected.ttl");
		Model generated = TestUtils.generateRDFSynchronously("./src/test/resources/bimr-tests/helio-1-mapping.json");
		
		Assert.assertTrue(TestUtils.compareModels(generated, expected));
		HelioMaterialiser.HELIO_CACHE.deleteGraphs();
		
	}
	
		
	@Test
	public void test2() throws IOException, MalformedMappingException, InterruptedException {
		HelioMaterialiser.HELIO_CACHE.deleteGraphs();
		
		String mappingStr = TestUtils.readFile("./src/test/resources/bimr-tests/helio-2-mapping.json");

		MappingTranslator translator = new JsonTranslator();
		HelioMaterialiserMapping mapping = translator.translate(mappingStr);
		
		HelioMaterialiser helio = new HelioMaterialiser(mapping);
	
		Model generated = ModelFactory.createDefaultModel();
		Set<String> irisFound = new HashSet<>();
		while(irisFound.size()< 3) {
			generated = helio.getRDF();
			Iterator<Statement> iterable = generated.listStatements(null, RDF.type, ResourceFactory.createResource("https://bimerr.iot.linkeddata.es/def/building#Building"));
			while(iterable.hasNext()) {
				String uri = iterable.next().asTriple().getSubject().getURI();
				irisFound.add(uri);
			}
			
		}
		Assert.assertTrue(irisFound.size() == 3);
		helio.close();
		HelioMaterialiser.HELIO_CACHE.deleteGraphs();
		
	}
	
	
	@Test
	public void test3() throws IOException, MalformedMappingException, InterruptedException {
		
		HelioMaterialiser.HELIO_CACHE.deleteGraphs();
		Model expected = TestUtils.readModel("./src/test/resources/bimr-tests/helio-3-expected.ttl");
		Model generated = TestUtils.generateRDFSynchronously("./src/test/resources/bimr-tests/helio-3-mapping.json");
		
		Assert.assertTrue(TestUtils.compareModels(generated, expected));
		HelioMaterialiser.HELIO_CACHE.deleteGraphs();
	}
	
	
	@Test
	public void test4() throws IOException, MalformedMappingException, InterruptedException {
		HelioMaterialiser.HELIO_CACHE.deleteGraphs();
		Model expected = TestUtils.readModel("./src/test/resources/bimr-tests/helio-4-expected.ttl");
		Model generated = TestUtils.generateRDFSynchronously("./src/test/resources/bimr-tests/helio-4-mapping.json");
		
		Assert.assertTrue(TestUtils.compareModels(generated, expected));
		HelioMaterialiser.HELIO_CACHE.deleteGraphs();
	}
	
	
	
	

}
