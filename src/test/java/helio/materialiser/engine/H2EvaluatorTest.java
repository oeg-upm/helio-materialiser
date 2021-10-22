package helio.materialiser.engine;

import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import helio.framework.expressions.EvaluableExpressionException;
import helio.framework.expressions.Expressions;
import helio.materialiser.evaluator.H2Evaluator;

public class H2EvaluatorTest {

	/**
	 * This method test the functions that replace with regex something using the characters '[' and ']'
	 * @throws EvaluableExpressionException is thrown parsing evaluable expressions with syntax errors
	 */
	@Test
	public void test01() throws EvaluableExpressionException {
		H2Evaluator EVALUATOR = new H2Evaluator();
		String expectedResult = "TVOCreplaced";
		List<String> result = EVALUATOR.evaluateExpresions(Expressions.create("[REGEXP_REPLACE('TVOC(Total Volatile Organic Compounds)', '\\([^\\)]+\\)','replaced')]"));
		Assert.assertTrue(result.get(0).equals(expectedResult));
	}
	
	/**
	 * This method tests evaluable expressions that have more than one function reference
	 * @throws EvaluableExpressionException is thrown parsing evaluable expressions with syntax errors
	 */
	@Test
	public void test02() throws EvaluableExpressionException {
		H2Evaluator EVALUATOR = new H2Evaluator();
		String expectedResult = "http://domain-sample.org/TVOCreplaced/data";
		List<String> result = EVALUATOR.evaluateExpresions(Expressions.create("http://domain-sample.org/[REGEXP_REPLACE('TVOC(Total Volatile Organic Compounds)', '\\([^\\)]+\\)','replaced')]/[TRIM(' data')]"));
		Assert.assertTrue(result.get(0).equals(expectedResult));
	}
	

	/**
	 * This method tests evaluable expressions that have one function reference that return more than one result
	 * @throws EvaluableExpressionException is thrown parsing evaluable expressions with syntax errors
	 */
	@Test
	public void test03() throws EvaluableExpressionException {
		H2Evaluator EVALUATOR = new H2Evaluator();
		
		List<String> expectedResult = new ArrayList<>();
		expectedResult.add("http://domain-sample.org/TVOC(Total");
		expectedResult.add("http://domain-sample.org/Volatile");
		expectedResult.add("http://domain-sample.org/Organic");
		expectedResult.add("http://domain-sample.org/Compounds)");
		
		List<String> result = EVALUATOR.evaluateExpresions(Expressions.create("http://domain-sample.org/[splitBy('TVOC(Total Volatile Organic Compounds)','\\s+')]"));
		Assert.assertTrue(result.containsAll(expectedResult));
		Assert.assertTrue(expectedResult.containsAll(result));
	}
	
	
	/**
	 * This method tests evaluable expressions that have two function reference that return more than one result
	 * @throws EvaluableExpressionException is thrown parsing evaluable expressions with syntax errors
	 */
	@Test
	public void test04() throws EvaluableExpressionException {
		H2Evaluator EVALUATOR = new H2Evaluator();
		
		List<String> expectedResult = new ArrayList<>();
		expectedResult.add("http://domain-sample.org/TVOC(Total/A");
		expectedResult.add("http://domain-sample.org/TVOC(Total/B");
		expectedResult.add("http://domain-sample.org/TVOC(Total/C");
		expectedResult.add("http://domain-sample.org/Volatile/A");
		expectedResult.add("http://domain-sample.org/Volatile/B");
		expectedResult.add("http://domain-sample.org/Volatile/C");
		expectedResult.add("http://domain-sample.org/Organic/A");
		expectedResult.add("http://domain-sample.org/Organic/B");
		expectedResult.add("http://domain-sample.org/Organic/C");
		expectedResult.add("http://domain-sample.org/Compounds)/A");
		expectedResult.add("http://domain-sample.org/Compounds)/B");
		expectedResult.add("http://domain-sample.org/Compounds)/C");
		
		List<String> result = EVALUATOR.evaluateExpresions(Expressions.create("http://domain-sample.org/[splitBy('TVOC(Total Volatile Organic Compounds)','\\s+')]/[splitBy('A B C', '\\s+')]"));
		
		Assert.assertTrue(result.containsAll(expectedResult));
		Assert.assertTrue(expectedResult.containsAll(result));
	}
	
	/**
	 * This method tests evaluable expressions that have three function reference that return more than one result
	 * @throws EvaluableExpressionException is thrown parsing evaluable expressions with syntax errors
	 */
	@Test
	public void test05() throws EvaluableExpressionException {
		H2Evaluator EVALUATOR = new H2Evaluator();
		
		List<String> expectedResult = new ArrayList<>();
		expectedResult.add("http://domain-sample.org/TVOC(Total/A/Y");
		expectedResult.add("http://domain-sample.org/TVOC(Total/B/Y");
		expectedResult.add("http://domain-sample.org/TVOC(Total/C/Y");
		expectedResult.add("http://domain-sample.org/Volatile/A/Y");
		expectedResult.add("http://domain-sample.org/Volatile/B/Y");
		expectedResult.add("http://domain-sample.org/Volatile/C/Y");
		expectedResult.add("http://domain-sample.org/Organic/A/Y");
		expectedResult.add("http://domain-sample.org/Organic/B/Y");
		expectedResult.add("http://domain-sample.org/Organic/C/Y");
		expectedResult.add("http://domain-sample.org/Compounds)/A/Y");
		expectedResult.add("http://domain-sample.org/Compounds)/B/Y");
		expectedResult.add("http://domain-sample.org/Compounds)/C/Y");
		
		expectedResult.add("http://domain-sample.org/TVOC(Total/A/Z");
		expectedResult.add("http://domain-sample.org/TVOC(Total/B/Z");
		expectedResult.add("http://domain-sample.org/TVOC(Total/C/Z");
		expectedResult.add("http://domain-sample.org/Volatile/A/Z");
		expectedResult.add("http://domain-sample.org/Volatile/B/Z");
		expectedResult.add("http://domain-sample.org/Volatile/C/Z");
		expectedResult.add("http://domain-sample.org/Organic/A/Z");
		expectedResult.add("http://domain-sample.org/Organic/B/Z");
		expectedResult.add("http://domain-sample.org/Organic/C/Z");
		expectedResult.add("http://domain-sample.org/Compounds)/A/Z");
		expectedResult.add("http://domain-sample.org/Compounds)/B/Z");
		expectedResult.add("http://domain-sample.org/Compounds)/C/Z");
		
		List<String> result = EVALUATOR.evaluateExpresions(Expressions.create("http://domain-sample.org/[splitBy('TVOC(Total Volatile Organic Compounds)','\\s+')]/[splitBy('A B C', '\\s+')]/[splitBy('Z Y', '\\s+')]"));
	
		Assert.assertTrue(result.containsAll(expectedResult));
		Assert.assertTrue(expectedResult.containsAll(result));
	}
	
}
