package helio.materialiser.test.utils;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;

import helio.framework.exceptions.MalformedMappingException;
import helio.framework.materialiser.MappingTranslator;
import helio.framework.materialiser.mappings.HelioMaterialiserMapping;
import helio.materialiser.HelioMaterialiser;
import helio.materialiser.configuration.HelioConfiguration;
import helio.materialiser.mappings.AutomaticTranslator;

public class TestUtils {
	
	public static Model readModel(String file) {
		FileInputStream out;
		Model expected = ModelFactory.createDefaultModel();
		try {
			out = new FileInputStream(file);
			expected = ModelFactory.createDefaultModel();
			expected.read(out, HelioConfiguration.DEFAULT_BASE_URI, "TURTLE");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	
		return expected;
	}
	
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
			}catch(Exception e) {
				e.printStackTrace();
			} 
			return data.toString();
	 }
	
	public static Model generateRDFSynchronously(String mappingFile) throws MalformedMappingException {
		String mappingContent = readFile(mappingFile);
		Model model = ModelFactory.createDefaultModel();

		MappingTranslator translator = new AutomaticTranslator();
		HelioMaterialiserMapping mapping = translator.translate(mappingContent);
			
		HelioMaterialiser helio = new HelioMaterialiser(mapping);
		helio.updateSynchronousSources();
		model = helio.getRDF();
		helio.close();
		
		return model;
	}
	
	public static Boolean compareModels(Model model1, Model model2) {
		Boolean correct = true;
		if(model1==null || model2==null)
			return false;
		
		correct &= model1.listStatements().toList().stream().allMatch(st -> model2.contains(null, st.getPredicate()));
		correct &= model1.listSubjects().toList().stream().filter(sub -> !sub.isAnon()).allMatch(sub -> model2.contains(sub, null));
		correct &= model1.listObjects().toList().stream().allMatch(obj1 -> model2.listObjects().toList().stream().anyMatch(obj2 -> compare(obj1,obj2)));
		return correct;
	}
	
	private static boolean compare(RDFNode obj1, RDFNode obj2) {
		Boolean equal = false;
		if(obj1.isLiteral() && obj2.isLiteral()) {
			equal = obj1.asLiteral().getLexicalForm().equals(obj2.asLiteral().getLexicalForm());
		}else if(obj1.isResource() && obj2.isResource() && !obj1.isAnon() && !obj2.isAnon()){
			equal = obj1.equals(obj2);
		}if( obj1.isAnon() && obj2.isAnon()) {
			equal = true;
		}
		
		return equal;
	}

}
