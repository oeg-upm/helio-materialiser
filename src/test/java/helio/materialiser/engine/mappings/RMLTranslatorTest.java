package helio.materialiser.engine.mappings;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.junit.Assert;
import org.junit.Test;

import helio.framework.exceptions.MalformedMappingException;
import helio.framework.materialiser.MappingTranslator;
import helio.framework.materialiser.mappings.HelioMaterialiserMapping;
import helio.materialiser.HelioMaterialiser;
import helio.materialiser.configuration.HelioConfiguration;
import helio.materialiser.mappings.AutomaticTranslator;

public class RMLTranslatorTest {
	
	
	public static final Resource[] iris = new Resource[] {};
	
	
	@Test
	public void testXML1() throws Exception {
		String mappingFile = "./src/test/resources/rml-tests/xml/test-xml-1-mapping.ttl";
		String expectedFile = "./src/test/resources/rml-tests/xml/test-xml-1-expected.ttl";
		
		FileInputStream out = new FileInputStream(expectedFile);
		Model expected = Rio.parse(out, HelioConfiguration.DEFAULT_BASE_URI, RDFFormat.TURTLE, iris);	
		//
		Model generated = generateRDF(mappingFile);
		//
		Boolean correct = expected.stream().allMatch(st -> generated.contains(st.getSubject(), st.getPredicate(), st.getObject(), iris));
		Assert.assertTrue(correct);
	}
	
	@Test
	public void testXML2() throws Exception {
		String mappingFile = "./src/test/resources/rml-tests/xml/test-xml-2-mapping.ttl";
		String expectedFile = "./src/test/resources/rml-tests/xml/test-xml-2-expected.ttl";
		FileInputStream out = new FileInputStream(expectedFile);
		Model expected = Rio.parse(out, HelioConfiguration.DEFAULT_BASE_URI, RDFFormat.TURTLE, iris);	
		//
		Model generated = generateRDF(mappingFile);
		//
		Boolean correct = expected.stream().allMatch(st -> generated.contains(st.getSubject(), st.getPredicate(), st.getObject(), iris));
		Assert.assertTrue(correct);
	}
	
	@Test
	public void testCSV1() throws Exception {
		String mappingFile = "./src/test/resources/rml-tests/csv/test-csv-1-mapping.ttl";
		String expectedFile = "./src/test/resources/rml-tests/csv/test-csv-1-expected.ttl";
		FileInputStream out = new FileInputStream(expectedFile);
		Model expected = Rio.parse(out, HelioConfiguration.DEFAULT_BASE_URI, RDFFormat.TURTLE, iris);	
		//
		Model generated = generateRDF(mappingFile);
		//
		Boolean correct = expected.stream().allMatch(st -> generated.contains(st.getSubject(), st.getPredicate(), st.getObject(), iris));
		Assert.assertTrue(correct);
	}
	
	@Test
	public void testJSON1() throws Exception {
		String mappingFile = "./src/test/resources/rml-tests/json/test-json-1-mapping.ttl";
		String expectedFile = "./src/test/resources/rml-tests/json/test-json-1-expected.ttl";
		FileInputStream out = new FileInputStream(expectedFile);
		Model expected = Rio.parse(out, HelioConfiguration.DEFAULT_BASE_URI, RDFFormat.TURTLE, iris);	
		//
		Model generated = generateRDF(mappingFile);
		//
		Boolean correct = expected.stream().allMatch(st -> generated.contains(st.getSubject(), st.getPredicate(), st.getObject(), iris));
		Assert.assertTrue(correct);
	}
	
	/*@Test
	public void testMixed1() throws Exception {
		String mappingFile = "./src/test/resources/rml-tests/mixed/test-mixed-1-mapping.ttl";
		String expectedFile = "./src/test/resources/rml-tests/mixed/test-mixed-1-expected.ttl";
		FileInputStream out = new FileInputStream(expectedFile);
		Model expected = Rio.parse(out, HelioConfiguration.DEFAULT_BASE_URI, RDFFormat.TURTLE, iris);	
		//
		Model generated = generateRDF(mappingFile);
		//
		Rio.write(generated, System.out, RDFFormat.TURTLE);
		//System.out.println("---");
		
		//generated.stream().forEach(st -> System.out.println(st));
		//System.out.println("---");
		//expected.stream().forEach(st -> System.out.println(st));
		Boolean correct = expected.stream().allMatch(st -> generated.contains(st.getSubject(), st.getPredicate(), st.getObject(), iris));
		Assert.assertTrue(correct);
	}*/
	
	@Test
	public void testMixed2() throws Exception {
		String mappingFile = "./src/test/resources/rml-tests/mixed/test-mixed-2-mapping.ttl";
		String expectedFile = "./src/test/resources/rml-tests/mixed/test-mixed-2-expected.ttl";
		FileInputStream out = new FileInputStream(expectedFile);
		Model expected = Rio.parse(out, HelioConfiguration.DEFAULT_BASE_URI, RDFFormat.TURTLE, iris);	
		//
		Model generated = generateRDF(mappingFile);
		//
		generated.forEach(st -> System.out.println(st));
		Boolean correct = expected.stream().allMatch(st -> generated.contains(st.getSubject(), st.getPredicate(), st.getObject(), iris));
		Assert.assertTrue(correct);
	}
	
	@Test
	public void testMixed3() throws Exception {
		String mappingFile = "./src/test/resources/rml-tests/mixed/test-mixed-3-mapping.ttl";
		//
		Boolean expeptionThrown = false;
		try {
			generateRDF(mappingFile);
		}catch (Exception e) {
			expeptionThrown = true;
		}
		//
		Assert.assertTrue(expeptionThrown);
	}
	
	
	// Ancillary methods

	 public static String readFile(String fileName) {
		 StringBuilder data = new StringBuilder();
			// 1. Read the file
			try {
				FileReader file = new FileReader(fileName);
				BufferedReader bf = new BufferedReader(file);
				// 2. Accumulate its lines in the data var
				bf.lines().forEach( line -> data.append(line).append("\n"));
				bf.close();
				file.close();
				// TODO: Opening and clossing im not sure is the best option 
			}catch(Exception e) {
				e.printStackTrace();
			} 
			return data.toString();
	 }


		private Model generateRDF(String mappingFile) throws MalformedMappingException {
			HelioMaterialiser.HELIO_CACHE.deleteGraphs();
			MappingTranslator translator = new AutomaticTranslator();
			
			String mappingStr = readFile(mappingFile);
			HelioMaterialiserMapping mapping = translator.translate(mappingStr);
			Model model = new LinkedHashModel();
			if(!mapping.getRuleSets().isEmpty()) {
			HelioMaterialiser materialiser = new HelioMaterialiser(mapping);
			materialiser.updateSynchronousSources();
			model.addAll( materialiser.getRDF());
			
			materialiser.close();
			
			HelioMaterialiser.HELIO_CACHE.deleteGraphs();
			}else {
				throw new MalformedMappingException("");
			}
			return model;
		}
	
}
