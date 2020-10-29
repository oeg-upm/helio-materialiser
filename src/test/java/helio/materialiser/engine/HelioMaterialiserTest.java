package helio.materialiser.engine;

import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;

import helio.framework.exceptions.MalformedMappingException;
import helio.framework.materialiser.mappings.HelioMaterialiserMapping;
import helio.framework.objects.SparqlResultsFormat;
import helio.materialiser.HelioMaterialiser;
import helio.materialiser.configuration.HelioConfiguration;
import helio.materialiser.mappings.JsonTranslator;

public class HelioMaterialiserTest {

	public static final String JSON_MAPPING = "{" +
			"  \"datasources\" : [\n" + 
			"      {\n" + 
			"        \"id\" : \"test\",\n" + 
			"        \"provider\" : { \"type\" : \"FileProvider\", \"file\" : \"./src/test/resources/handlers-tests/json/json-file-1.json\"},\n" + 
			"        \"handler\" : { \"type\" : \"JsonHandler\", \"iterator\" : \"$.book[*]\"}\n" + 
			"      }\n" + 
			"  ],\n" + 
			"\"resource_rules\" : [\n" + 
			"    { \n" + 
			"      \"id\" : \"Astrea Queries\",\n" + 
			"      \"datasource_ids\" : [\"test\"],\n" + 
			"      \"subject\" : \"http://localhost:8080/[REPLACE(TRIM({$.title}), ' ', '')]\",\n" + 
			"      \"properties\"  : [\n" + 
			"            {\n" + 
			"               \"predicate\" : \"http://www.w3.org/1999/02/22-rdf-syntax-ns#type\", \n" + 
			"               \"object\" : \"https://w3id.org/def/astrea#SPARQLQuery\",\n" + 
			"               \"is_literal\" : \"False\" \n" + 
			"            },\n" + 
			"            {\n" + 
			"               \"predicate\" : \"https://w3id.org/def/astrea#body\", \n" + 
			"               \"object\" : \"[TRIM({$.title})]\",\n" + 
			"               \"lang\" : \"en\",\n" + 
			"               \"is_literal\" : \"True\" \n" + 
			"            },{\n" + 
			"               \"predicate\" : \"https://w3id.org/def/astrea#order\", \n" + 
			"               \"object\" : \"{$.price}\",\n" + 
			"                \"datatype\" : \"http://www.w3.org/2001/XMLSchema#nonNegativeInteger\",\n" + 
			"               \"is_literal\" : \"True\" \n" + 
			"            }                       \n" + 
			"      ]\n" + 
			"    }" +
			"  ]\n" + 
			"} " ;
	

			
	
	@Test
	public void testAddData() throws IOException, MalformedMappingException, InterruptedException {
		HelioConfiguration.HELIO_CACHE.deleteGraphs();
		JsonTranslator translator = new JsonTranslator();
		HelioMaterialiserMapping mappings = translator.translate(JSON_MAPPING);
		HelioMaterialiser helio = new HelioMaterialiser(mappings);
	
		
		
		helio.updateSynchronousSources();
		
		//Thread.sleep(200);
		

		Assert.assertTrue(!HelioConfiguration.HELIO_CACHE.getGraphs().isEmpty());
		
	}
	
	
	@Test
	public void testAddDataAndQuery() throws IOException, MalformedMappingException, InterruptedException {
		JsonTranslator translator = new JsonTranslator();
		HelioMaterialiserMapping mappings = translator.translate(JSON_MAPPING);
		
		HelioMaterialiser helio = new HelioMaterialiser(mappings);
		
		
		
		HelioConfiguration.HELIO_CACHE.deleteGraphs();
		Assert.assertTrue(HelioConfiguration.HELIO_CACHE.getGraphs().isEmpty());
		
		helio.updateSynchronousSources();

		String  input = HelioConfiguration.HELIO_CACHE.solveTupleQuery("SELECT DISTINCT ?s { ?s ?p ?o .}", SparqlResultsFormat.JSON);
		
		Assert.assertFalse(input.toString().isEmpty());
		
	}

	
	
	public static final String JSON_MAPPING_2 = "{" +
			"  \"datasources\" : [\n" + 
			"      {\n" + 
			"        \"id\" : \"test\",\n" + 
			"        \"provider\" : { \"type\" : \"FileProvider\", \"file\" : \"./src/test/resources/handlers-tests/json/json-file-2.json\"},\n" + 
			"        \"handler\" : { \"type\" : \"JsonHandler\", \"iterator\" : \"$.book[*]\"}\n" + 
			"      }\n" + 
			"  ],\n" + 
			"\"resource_rules\" : [\n" + 
			"    { \n" + 
			"      \"id\" : \"Astrea Queries\",\n" + 
			"      \"datasource_ids\" : [\"test\"],\n" + 
			"      \"subject\" : \"http://localhost:8080/[REPLACE(TRIM({$.title}), ' ', '')]\",\n" + 
			"      \"properties\"  : [\n" + 
			"            {\n" + 
			"               \"predicate\" : \"http://www.w3.org/1999/02/22-rdf-syntax-ns#type\", \n" + 
			"               \"object\" : \"https://w3id.org/def/astrea#SPARQLQuery\",\n" + 
			"               \"is_literal\" : \"False\" \n" + 
			"            },\n" + 
			"            {\n" + 
			"               \"predicate\" : \"https://w3id.org/def/astrea#body\", \n" + 
			"               \"object\" : \"[TRIM({$.title})]\",\n" + 
			"               \"lang\" : \"en\",\n" + 
			"               \"is_literal\" : \"True\" \n" + 
			"            },{\n" + 
			"               \"predicate\" : \"https://w3id.org/def/astrea#order\", \n" + 
			"               \"object\" : \"{$.price}\",\n" + 
			"                \"datatype\" : \"http://www.w3.org/2001/XMLSchema#nonNegativeInteger\",\n" + 
			"               \"is_literal\" : \"True\" \n" + 
			"            }                       \n" + 
			"      ]\n" + 
			"    }" +
			"  ]\n" + 
			"} " ;
	
	
	@Test
	public void duplicatedSubjects() throws IOException, MalformedMappingException, InterruptedException {
		JsonTranslator translator = new JsonTranslator();
		HelioMaterialiserMapping mappings = translator.translate(JSON_MAPPING_2);
		
		HelioMaterialiser helio = new HelioMaterialiser(mappings);
	
		HelioConfiguration.HELIO_CACHE.deleteGraphs();
		Assert.assertTrue(HelioConfiguration.HELIO_CACHE.getGraphs().isEmpty());
		
		helio.updateSynchronousSources();
		helio.getRDF().write(System.out, "TTL");
		
		//Assert.assertFalse(input.toString().isEmpty());
		
	}
	

}
