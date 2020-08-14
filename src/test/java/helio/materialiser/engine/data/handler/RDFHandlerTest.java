package helio.materialiser.engine.data.handler;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
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

public class RDFHandlerTest  {

	
	@Test
	public void rdfHandlerTest() throws Exception {
		// Default separator ; and no delimiter, and column names
		DataProvider fileProvider = new FileProvider(new File("./src/test/resources/test-rdf.ttl"));
		
		JsonObject configuration = new JsonObject();
		configuration.addProperty("mime", "text/turtle");
		RDFHandler handler = new RDFHandler();
		handler.configure(configuration);
		Queue<String> data = handler.splitData(fileProvider.getData());
		int queuedDataSize = data.size();
		String polledData = data.poll();
		Resource[] iris = new Resource[] {};
		InputStream inputStream = new ByteArrayInputStream(polledData.getBytes(Charset.forName("UTF-8")));
		Model model = Rio.parse(inputStream, HelioConfiguration.DEFAULT_BASE_URI, HelioConfiguration.DEFAULT_RDF_FORMAT, iris);
		String subject = "http://helio.linkeddata.es/Instance_1";
		String predicate = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
		String object = "http://id.loc.gov/ontologies/bibframe.rdfInstance";
		Boolean contained = model.stream().findFirst().filter(st -> st.getSubject().stringValue().equals(subject) && st.getPredicate().stringValue().equals(predicate) && st.getObject().stringValue().equals(object) ).isPresent();
		Assert.assertTrue(contained && queuedDataSize==1); // checking the size allows knowing that bnodes as subjects are appended in the RDF document which referenced them as object
	}

	@Test
	public void rdfHandlerMaterialisationTest() throws Exception {
		DataProvider fileProvider = new FileProvider(new File("./src/test/resources/test-rdf.ttl"));
		
		JsonObject configuration = new JsonObject();
		configuration.addProperty("mime", "text/turtle");
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
		
		ValueFactory valueFactory = SimpleValueFactory.getInstance();
		IRI[] iris = new IRI[] {};
		Boolean contained = helio.getRDF(RDFFormat.TURTLE).contains(valueFactory.createIRI("http://helio.linkeddata.es/Instance_1"), valueFactory.createIRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"), valueFactory.createIRI("http://id.loc.gov/ontologies/bibframe.rdfInstance"), iris);
		contained &= helio.getRDF(RDFFormat.TURTLE).contains(null, valueFactory.createIRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"), valueFactory.createIRI("http://id.loc.gov/ontologies/bibframe.rdfProperty"), iris);
		contained &= helio.getRDF(RDFFormat.TURTLE).contains(null, valueFactory.createIRI("http://id.loc.gov/ontologies/bibframe.rdfMusicNotation"), valueFactory.createIRI("http://rdaregistry.info/termList/MusNotation/1007"), iris);
		
		Assert.assertTrue(contained);
	}
	

	
}
