 package helio.materialiser.engine;

import java.io.IOException;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.junit.Assert;
import org.junit.Test;

import helio.framework.exceptions.MalformedMappingException;
import helio.framework.materialiser.MappingTranslator;
import helio.framework.materialiser.mappings.HelioMaterialiserMapping;
import helio.framework.objects.SparqlResultsFormat;
import helio.materialiser.HelioMaterialiser;
import helio.materialiser.configuration.HelioConfiguration;
import helio.materialiser.mappings.AutomaticTranslator;
import helio.materialiser.test.utils.TestUtils;

public class ThemisTest {
	
	/*private static final String CONFIGURATION = "@prefix sparql: <http://www.openrdf.org/config/repository/sparql#> .\n" + 
			"@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>.\n" + 
			"@prefix rep: <http://www.openrdf.org/config/repository#>.\n" + 
			"@prefix sr: <http://www.openrdf.org/config/repository/sail#>.\n" + 
			"@prefix sail: <http://www.openrdf.org/config/sail#>.\n" + 
			"@prefix ms: <http://www.openrdf.org/config/sail/memory#>.\n" + 
			"\n" + 
			"[] a rep:Repository ;\n" + 
			"   rep:repositoryID \"helio-storage\" ; # this id must be the same id in the Helio configuration file $.repository.id\n" + 
			"   rep:repositoryImpl [\n" + 
			"     <http://www.openrdf.org/config/repository#repositoryType> \"openrdf:SPARQLRepository\";\n" + 
			"  		sparql:query-endpoint <http://localhost:7200/repositories/discovery>; # change this IRI for the corrent endpoint sparql for querying\n" + 
			"  		sparql:update-endpoint <http://localhost:7200/repositories/discovery/statements> ; # change this IRI for the corrent endpoint sparql for updating\n" + 
			"   ].\n" + 
			"";
*/
	
	@Test
	public void test1() throws IOException, MalformedMappingException, InterruptedException {
		HelioConfiguration.HELIO_CACHE.deleteGraphs();
		//HelioConfiguration.HELIO_CACHE.configureRepository(CONFIGURATION);
		Model expected = TestUtils.readModel("./src/test/resources/themis-tests/themis-1-expected.ttl");
		Model generated = TestUtils.generateRDFSynchronously("./src/test/resources/themis-tests/themis-1-mapping.json");
		
		Assert.assertTrue(TestUtils.compareModels(generated, expected));
		HelioConfiguration.HELIO_CACHE.deleteGraphs();
		
	}
	
	@Test
	public void test2() throws IOException, MalformedMappingException, InterruptedException {
		HelioConfiguration.HELIO_CACHE.deleteGraphs();
		//HelioConfiguration.HELIO_CACHE.configureRepository(CONFIGURATION);
		Model expected = TestUtils.readModel("./src/test/resources/themis-tests/themis-1-expected.ttl");
		String mappingFile = "./src/test/resources/themis-tests/themis-1-mapping.json";
		String mappingContent = TestUtils.readFile(mappingFile);
		Model generated = ModelFactory.createDefaultModel();

		MappingTranslator translator = new AutomaticTranslator();
		HelioMaterialiserMapping mapping = translator.translate(mappingContent);
			
		HelioMaterialiser helio = new HelioMaterialiser(mapping);
		helio.updateSynchronousSources();
		//System.out.println(helio.query("SELECT ?s { ?s a ?types }", SparqlResultsFormat.CSV));
		System.out.println(helio.query("ASK { ?s <http://xmlns.com/foaf/0.1/name> ?types }", SparqlResultsFormat.XML));
		//System.out.println(helio.query("CONSTRUCT { ?s a ?types } where { ?s a ?o .}", SparqlResultsFormat.RDF_TTL));
		//System.out.println(helio.query("DESCRIBE ?s { ?s a ?types }", SparqlResultsFormat.RDF_TTL));
		generated = helio.getRDF();
		helio.close();
		
		
		
		Assert.assertTrue(TestUtils.compareModels(generated, expected));
		HelioConfiguration.HELIO_CACHE.deleteGraphs();
		
	}

}
