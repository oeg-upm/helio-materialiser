package helio.materialiser.engine;

import org.apache.jena.rdf.model.Model;
import org.junit.Assert;
import org.junit.Test;

import helio.framework.exceptions.MalformedMappingException;
import helio.materialiser.configuration.HelioConfiguration;
import helio.materialiser.test.utils.TestUtils;

public class DataTypesTest {
	@Test
	public void createOneDataType() throws MalformedMappingException {
		HelioConfiguration.HELIO_CACHE.deleteGraphs();
		Model expected = TestUtils.readModel("./src/test/resources/datatypes-tests/createOneDataType-expected.ttl");
		Model generated = TestUtils.generateRDFSynchronously("./src/test/resources/datatypes-tests/createOneDataType-mapping.json");
		generated.listStatements().forEachRemaining(st -> System.out.println(st));
		Assert.assertTrue(TestUtils.compareModels(generated, expected));
		HelioConfiguration.HELIO_CACHE.deleteGraphs();
	}
	
//TODO: Create a function to transform an array of string to URI, otherwise an array of string cannot be translated into multiple datatypes
//	@Test
//	public void createMultipleDataType() {		
//		HelioConfiguration.HELIO_CACHE.deleteGraphs();
//		Model expected = TestUtils.readModel("./src/test/resources/datatypes-tests/createMultipleDataType-expected.ttl");
//		Model generated = TestUtils.generateRDFSynchronously("./src/test/resources/datatypes-tests/createMultipleDataType-mapping.json");
//		generated.listStatements().forEachRemaining(st -> System.out.println(st));
//		Assert.assertTrue(TestUtils.compareModels(generated, expected));
//		HelioConfiguration.HELIO_CACHE.deleteGraphs();
//	}
//	
//	@Test
//	public void noDataType() {
//		HelioConfiguration.HELIO_CACHE.deleteGraphs();
//		Model expected = TestUtils.readModel("./src/test/resources/datatypes-tests/noDataType-expected.ttl");
//		Model generated = TestUtils.generateRDFSynchronously("./src/test/resources/datatypes-tests/noDataType-mapping.json");
//		
//		Assert.assertTrue(TestUtils.compareModels(generated, expected));
//		HelioConfiguration.HELIO_CACHE.deleteGraphs();
//	}
}
