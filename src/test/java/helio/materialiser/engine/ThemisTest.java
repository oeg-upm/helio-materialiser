package helio.materialiser.engine;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.junit.Assert;
import org.junit.Test;

import helio.framework.exceptions.MalformedMappingException;
import helio.framework.materialiser.MappingTranslator;
import helio.framework.materialiser.mappings.HelioMaterialiserMapping;
import helio.materialiser.HelioMaterialiser;
import helio.materialiser.configuration.HelioConfiguration;
import helio.materialiser.mappings.JsonTranslator;
import helio.materialiser.test.utils.TestUtils;

public class ThemisTest {
	

	
	@Test
	public void test1() throws IOException, MalformedMappingException, InterruptedException {
		HelioMaterialiser.HELIO_CACHE.deleteGraphs();
		Model expected = TestUtils.readModel("./src/test/resources/themis-tests/themis-1-expected.ttl");
		Model generated = TestUtils.generateRDFSynchronously("./src/test/resources/themis-tests/themis-1-mapping.json");
		
		Assert.assertTrue(TestUtils.compareModels(generated, expected));
		HelioMaterialiser.HELIO_CACHE.deleteGraphs();
		
	}
	

}
