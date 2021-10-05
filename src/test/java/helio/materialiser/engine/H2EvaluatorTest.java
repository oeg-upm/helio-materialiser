package helio.materialiser.engine;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import helio.framework.exceptions.MalformedMappingException;
import helio.framework.materialiser.mappings.HelioMaterialiserMapping;
import helio.materialiser.MaterialiserOrchestrator;
import helio.materialiser.configuration.HelioConfiguration;
import helio.materialiser.evaluator.H2Evaluator;
import helio.materialiser.mappings.JsonTranslator;

public class H2EvaluatorTest {

	
	@Test
	public void testAddData() throws IOException, MalformedMappingException, InterruptedException {
		H2Evaluator EVALUATOR = new H2Evaluator();
		String result = EVALUATOR.evaluateExpresions("[REGEXP_REPLACE('TVOC(Total Volatile Organic Compounds)', '\\([^\\)]+\\)','')]");
		
		Assert.assertFalse(result.equals("TVOC"));
		
	}
}
