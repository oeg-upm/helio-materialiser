package helio.materialiser.issues;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.XSD;
import org.junit.Assert;
import org.junit.Test;

import helio.framework.exceptions.MalformedMappingException;
import helio.materialiser.configuration.HelioConfiguration;
import helio.materialiser.test.utils.TestUtils;

public class GithubIssuesTest {

//	@Test
//	public void testIssue7() throws MalformedMappingException {
//		HelioConfiguration.HELIO_CACHE.deleteGraphs();
//		Model generated = TestUtils.generateRDFSynchronously("./src/test/resources/git-issues/issue7/mapping-issue7.json");
//		Model expectedModel = TestUtils.readModel("./src/test/resources/git-issues/issue7/expected-rdf.ttl");
//		Assert.assertTrue(TestUtils.compareModels(generated, expectedModel));
//		Assert.assertTrue(!generated.isEmpty());
//		HelioConfiguration.HELIO_CACHE.deleteGraphs();
//	}
//
//	
//	/**
//	 * This method test the generation of subjects using the function current time stamp
//	 * @throws MalformedMappingException if the provided mapping has syntax errors
//	 */
//	@Test
//	public void testIssue8() throws MalformedMappingException {
//		HelioConfiguration.HELIO_CACHE.deleteGraphs();
//		Model generated = TestUtils.generateRDFSynchronously("./src/test/resources/git-issues/issue8/mapping-issue8.json");
//		
//		Boolean contains = generated.contains(null, RDF.type, (RDFNode) ResourceFactory.createResource("https://bimerr.iot.linkeddata.es/def/building#Building"));
//		String literal = generated.listObjectsOfProperty(ResourceFactory.createProperty("https://bimerr.iot.linkeddata.es/def/building#description")).nextNode().asLiteral().toString();
//		contains &= literal.equals("A office building which contains 12 space and 16 staffs.^^http://www.w3.org/2001/XMLSchema#string");
//		contains &= generated.contains(null, ResourceFactory.createProperty("https://w3id.org/def/saref4bldg#hasSpace"), (RDFNode) ResourceFactory.createResource("https://www.data.bimerr.occupancy.es/resource/S2_Researcher_Office"));
//		Assert.assertTrue(contains);
//		HelioConfiguration.HELIO_CACHE.deleteGraphs();
//	}
//
//	// https://github.com/oeg-upm/helio/issues/14
//	// This test takes ~1h to be ran
//	@Test
//	public void testIssue14() throws MalformedMappingException, IOException {
//		long startTimeTotal = System.nanoTime();
//		long startTime = System.nanoTime();
//		HelioConfiguration.HELIO_CACHE.deleteGraphs();
//		long stopTime = System.nanoTime();
//		System.out.println("Deleting graphs time: "+(stopTime - startTime));
//		startTime = System.nanoTime();
//		Model generated = TestUtils.generateRDFSynchronously("./src/test/resources/git-issues/issue14/rml-mapping.txt");
//		stopTime = System.nanoTime();
//		System.out.println("Generation time: "+(stopTime - startTime));
//		Assert.assertTrue(!generated.isEmpty());
//		Model expectedModel = TestUtils.readModel("./src/test/resources/git-issues/issue14/expected-rdf.ttl");
//		Assert.assertTrue(TestUtils.compareModels(generated, expectedModel));
//		startTime = System.nanoTime();
//		HelioConfiguration.HELIO_CACHE.deleteGraphs();
//		stopTime = System.nanoTime();
//		System.out.println("Deleting graphs time: "+(stopTime - startTime));
//		long stopTimeTotal = System.nanoTime();
//		System.out.println("Total time: "+(stopTimeTotal - startTimeTotal));
//	}
	

}
