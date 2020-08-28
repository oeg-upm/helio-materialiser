package helio.materialiser.engine;

import java.io.IOException;

import org.apache.jena.rdf.model.Model;
import org.junit.Assert;
import org.junit.Test;

import helio.framework.exceptions.MalformedMappingException;
import helio.materialiser.configuration.HelioConfiguration;
import helio.materialiser.test.utils.TestUtils;

public class ThemisTest {
	

	
	@Test
	public void test1() throws IOException, MalformedMappingException, InterruptedException {
		HelioConfiguration.HELIO_CACHE.deleteGraphs();
		Model expected = TestUtils.readModel("./src/test/resources/themis-tests/themis-1-expected.ttl");
		Model generated = TestUtils.generateRDFSynchronously("./src/test/resources/themis-tests/themis-1-mapping.json");
		
		Assert.assertTrue(TestUtils.compareModels(generated, expected));
		HelioConfiguration.HELIO_CACHE.deleteGraphs();
		
	}
	

}
