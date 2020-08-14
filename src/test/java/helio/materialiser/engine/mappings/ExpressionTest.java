package helio.materialiser.engine.mappings;

import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

import helio.framework.materialiser.mappings.EvaluableExpression;

public class ExpressionTest {

	
	
	@Test
	public void oneDataReference() {
		Map<String,String> maps = new HashMap<>();
		maps.put("$.name", "Andrea");
		EvaluableExpression expression = new EvaluableExpression("The text is {$.name}"); 
		String evaluatedExpression = expression.instantiateExpression(maps);
		System.out.println(evaluatedExpression);
		System.out.println(expression.getExpression());
		Assert.assertTrue(evaluatedExpression.equals("The text is {Andrea}"));
	}
	
	@Test
	public void twoDataReference() {
		Map<String,String> maps = new HashMap<>();
		maps.put("$.name", "Andrea");
		maps.put("$.surname", "Cimmino");
		EvaluableExpression expression = new EvaluableExpression("The text is {$.name}, {$.surname} "); 
		String evaluatedExpression = expression.instantiateExpression(maps);
		System.out.println(expression.getExpression());
		Assert.assertTrue(evaluatedExpression.equals("The text is {Andrea}, {Cimmino} "));
	}
	
	@Test
	public void threeDataReferences() {
		Map<String,String> maps = new HashMap<>();
		maps.put("$.name", "Andrea");
		maps.put("$.surname", "Cimmino");
		maps.put("$.adr", "Madrid");
		EvaluableExpression expression = new EvaluableExpression("The text is {$.name}, {$.surname} - Address: {$.adr} "); 
		String evaluatedExpression = expression.instantiateExpression(maps);
		Assert.assertTrue(evaluatedExpression.equals("The text is {Andrea}, {Cimmino} - Address: {Madrid} "));
	}
}
