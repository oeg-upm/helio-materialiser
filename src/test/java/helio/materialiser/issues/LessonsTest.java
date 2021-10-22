package helio.materialiser.issues;

import org.apache.jena.rdf.model.Model;
import org.junit.Assert;
import org.junit.Test;

import helio.framework.exceptions.MalformedMappingException;
import helio.materialiser.configuration.HelioConfiguration;
import helio.materialiser.test.utils.TestUtils;

public class LessonsTest {

	
	
	@Test
	public void testIssue01() throws MalformedMappingException {
		HelioConfiguration.HELIO_CACHE.deleteGraphs();
		Model generated = TestUtils.generateRDFSynchronously("./src/test/resources/lessons/test01/accidents.ttl");
		Model expectedModel = TestUtils.readModel("./src/test/resources/lessons/test01/expected-rdf.ttl");
		Assert.assertTrue(TestUtils.compareModels(generated, expectedModel));
		Assert.assertTrue(!generated.isEmpty());
		HelioConfiguration.HELIO_CACHE.deleteGraphs();
	}
	
	@Test
	public void testIssue02() throws MalformedMappingException {
		HelioConfiguration.HELIO_CACHE.deleteGraphs();
		Model generated = TestUtils.generateRDFSynchronously("./src/test/resources/lessons/test02/SoloUnTripleMapRML.ttl");
		generated.write(System.out, "TTL");
		Model expectedModel = TestUtils.readModel("./src/test/resources/lessons/test02/expected-rdf.ttl");
		Assert.assertTrue(TestUtils.compareModels(generated, expectedModel));
		Assert.assertTrue(!generated.isEmpty());
		HelioConfiguration.HELIO_CACHE.deleteGraphs();
	}
	
}
