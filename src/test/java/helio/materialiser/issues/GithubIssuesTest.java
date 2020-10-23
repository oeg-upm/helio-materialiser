package helio.materialiser.issues;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.junit.Assert;
import org.junit.Test;

import helio.framework.exceptions.MalformedMappingException;
import helio.materialiser.configuration.HelioConfiguration;
import helio.materialiser.test.utils.TestUtils;

public class GithubIssuesTest {

	
	@Test
	public void testIssue8() throws MalformedMappingException {
		HelioConfiguration.HELIO_CACHE.deleteGraphs();
		Model generated = TestUtils.generateRDFSynchronously("./src/test/resources/git-issues/mapping-issue7.json");
		generated.write(System.out, "TTL");
		Assert.assertTrue(TestUtils.compareModels(generated, ModelFactory.createDefaultModel()));
		HelioConfiguration.HELIO_CACHE.deleteGraphs();
	}
	
	@Test
	public void testIssue7() throws MalformedMappingException {
		HelioConfiguration.HELIO_CACHE.deleteGraphs();
		Model generated = TestUtils.generateRDFSynchronously("./src/test/resources/git-issues/mapping-issue7.json");
		generated.write(System.out, "TTL");
		Assert.assertTrue(TestUtils.compareModels(generated, ModelFactory.createDefaultModel()));
		HelioConfiguration.HELIO_CACHE.deleteGraphs();
	}
	
}
