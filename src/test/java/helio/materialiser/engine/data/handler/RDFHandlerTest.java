package helio.materialiser.engine.data.handler;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.junit.Assert;
import org.junit.Test;

import com.google.gson.JsonObject;

import helio.framework.materialiser.mappings.DataProvider;
import helio.framework.materialiser.mappings.DataSource;
import helio.framework.materialiser.mappings.HelioMaterialiserMapping;
import helio.framework.materialiser.mappings.RuleSet;
import helio.materialiser.HelioMaterialiser;
import helio.materialiser.configuration.HelioConfiguration;
import helio.materialiser.data.handlers.RDFHandler;
import helio.materialiser.data.providers.FileProvider;
import helio.materialiser.test.utils.TestUtils;

public class RDFHandlerTest  {

	
	@Test
	public void rdfHandlerTest() throws Exception {
		HelioConfiguration.HELIO_CACHE.deleteGraphs();
		// Default separator ; and no delimiter, and column names
		Model expected = TestUtils.readModel("./src/test/resources/handlers-tests/rdf/test-rdf.ttl");
		
		DataProvider fileProvider = new FileProvider(new File("./src/test/resources/handlers-tests/rdf/test-rdf.ttl"));
		JsonObject configuration = new JsonObject();
		configuration.addProperty("format", "TURTLE");
		RDFHandler handler = new RDFHandler();
		handler.configure(configuration);
		
		
		Queue<String> data = handler.splitData(fileProvider.getData());
		String polledData = data.poll();
		InputStream inputStream = new ByteArrayInputStream(polledData.getBytes(Charset.forName("UTF-8")));
		Model parsedData = ModelFactory.createDefaultModel();
		parsedData.read(inputStream, HelioConfiguration.DEFAULT_BASE_URI, "TURTLE");
		
		Assert.assertTrue(TestUtils.compareModels(parsedData, expected));
		HelioConfiguration.HELIO_CACHE.deleteGraphs();
	}
	

	@Test
	public void rdfHandlerMaterialisationTest() throws Exception {
		HelioConfiguration.HELIO_CACHE.deleteGraphs();
		DataProvider fileProvider = new FileProvider(new File("./src/test/resources/handlers-tests/rdf/test-rdf.ttl"));
		
		JsonObject configuration = new JsonObject();
		configuration.addProperty("format", "TURTLE");
		RDFHandler handler = new RDFHandler();
		handler.configure(configuration);
		DataSource ds = new DataSource();
		ds.setId("test");
		ds.setDataHandler(handler);
		ds.setDataProvider(fileProvider);
		
		RuleSet rs = new RuleSet();
		rs.setResourceRuleId("test resources");
		Set<String> ids = new HashSet<>();
		ids.add("test");
		rs.setDatasourcesId(ids);
		HelioMaterialiserMapping mappings = new HelioMaterialiserMapping();
		mappings.getDatasources().add(ds);
		mappings.getRuleSets().add(rs);
		
		HelioMaterialiser helio = new HelioMaterialiser(mappings);
		helio.updateSynchronousSources();
		

		Model expected = TestUtils.readModel("./src/test/resources/handlers-tests/rdf/test-rdf.ttl");
		Model generated = helio.getRDF();
		Assert.assertTrue(TestUtils.compareModels(expected, generated));
		HelioConfiguration.HELIO_CACHE.deleteGraphs();
	}
	

	
}
