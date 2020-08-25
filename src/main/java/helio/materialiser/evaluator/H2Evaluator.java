package helio.materialiser.evaluator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import helio.framework.materialiser.Evaluator;
import helio.framework.materialiser.mappings.EvaluableExpression;
import helio.framework.materialiser.mappings.LinkRule;
import helio.materialiser.configuration.HelioConfiguration;
import helio.materialiser.executors.ExecutableLinkRule;

/**
 * H2Evaluator allows to evaluate expressions, a composite of functions that can return a {@link String} value as result or a {@link Boolean} value (a predicate expression)
 * <p>
 * H2Evaluator is based on the H2 and Hikari database engine, @see <a href="http://www.h2database.com/html/functions.html#length">Documentation</a> link for check the already implemented functions
 * <p>
 * To extend H2Evaluator functions a class must extend the abstract {@link Functions} class, and then the method implementing the desired functionality must be static. All the static function in such new class will be registered in this evaluator and will be invokable
 * 
 * @author Andrea Cimmino
 *
 */
public class H2Evaluator implements Evaluator {
	
	// -- Attributes
	private HikariDataSource datasource;
	private static Logger logger = LogManager.getLogger(H2Evaluator.class);

	
	// -- Constructor
	/**
	 *  Constructor of this class
	 */
	public H2Evaluator() {
		super();
		this.datasource = createInitHikari();
		loadProcedures();
	}
	
	// -- Methods
	
	/**
	 * This method initializes a Hikari datasource in-memory
	 * @return A {@link HikariDataSource} object
	 */
	private static HikariDataSource createInitHikari() {
		HikariConfig config = new HikariConfig();
		config.setDataSourceClassName("org.h2.jdbcx.JdbcDataSource");
		config.setConnectionTestQuery("VALUES 1");
		config.setAutoCommit(true);
		config.setAllowPoolSuspension(true);
		//config.addDataSourceProperty("URL", "jdbc:h2:mem:semantic-engine;MULTI_THREADED=TRUE;CACHE_SIZE=2048;DB_CLOSE_ON_EXIT=FALSE");
		config.addDataSourceProperty("URL", "jdbc:h2:file:./"+HelioConfiguration.PERSISTENT_CACHE_DIRECTORY+"/semantic-engine-h2-cache;MULTI_THREADED=TRUE;CACHE_SIZE=2048;DB_CLOSE_ON_EXIT=TRUE");
		
		config.setMaximumPoolSize(50);
		return new HikariDataSource(config);
	}
	
	// methods to store functions
	
	/**
	 * This method register in the hikari datasource all the procedures implemented with java code that extends the {@link Function} abstract class
	 */
	private void loadProcedures() {
		// 1. Find all classes that extends Function
		/*Reflections reflections = new Reflections("helio.components.evaluator");
		Set<Class<? extends Functions>> classes = reflections.getSubTypesOf(Functions.class);
		classes.addAll(PluginDiscovery.getFunctionsclasses());
		// 2. For each class retrieve their static methods
		for (Class<? extends Functions> clazz : classes) {
			Method[] methods = clazz.getMethods();
			for (Method method : methods) {
				if (Modifier.isStatic(method.getModifiers())) {
					// 2.1.1.A If the method is static register it in the datasource
					StringBuilder builder = new StringBuilder();
					builder.append("CREATE ALIAS IF NOT EXISTS ").append(method.getName());
					builder.append(" FOR \"").append(clazz.getName()).append(".").append(method.getName()).append("\"");
					log.info("From "+clazz.getSimpleName()+" registering " + method.getName());
					registerFunction(builder.toString());
				}
			}
		}*/
	}

	/**
	 * This method registers a procedure creation statement into the datasource
	 * @param function A procedure creation statement
	 */
	private void registerFunction(String function) {
		try {
			Connection connection = datasource.getConnection();
			connection.setAutoCommit(true);
			PreparedStatement result  = connection.prepareStatement(function);
			result.setFetchSize(1000);
			result.executeUpdate();
			result.close();
			connection.close();	
			
		} catch (SQLException e) {
			logger.error(e.toString());
		}
	}

	// Linking methods

	public void initH2Cache() {
		closeH2Cache(); // first delete table
		Connection con = null;
		PreparedStatement pst = null;
		String query = "CREATE TABLE IF NOT EXISTS LINKRULES(ID IDENTITY PRIMARY KEY, LINKING_ID INT, SOURCE VARCHAR(255), TARGET VARCHAR(255), EXPRESSION VARCHAR(255), SOURCE_VALUES VARCHAR(255), TARGET_VALUES VARCHAR(255), PREDICATE VARCHAR(255), INVERSE_PREDICATE VARCHAR(255));";
		try {
			con = datasource.getConnection();
			pst = con.prepareStatement(query);
			pst.executeUpdate();
		} catch (SQLException ex) {
			System.out.println(">*>"+query);
			ex.printStackTrace();
		} finally {
			try {
				if (pst != null) {
					pst.close();
				}
				if (con != null) {
					con.close();
				}
				
			} catch (SQLException ex) {
				ex.toString();
			}
		}
	}
	
	public void updateCache(String sourceSubject, String targetSubject, String sourceValues, String targetValues, LinkRule linkRule) {
		Connection con = null;
		PreparedStatement pst = null;
		String predicate = linkRule.getPredicate();
		String inversePredicate = linkRule.getInversePredicate();
		Integer linkingId = (linkRule.getSourceNamedGraph()+linkRule.getTargetNamedGraph()).hashCode();
		String expression = linkRule.getExpression().getExpression();
		String query = "INSERT INTO LINKRULES(LINKING_ID, SOURCE, TARGET, EXPRESSION, SOURCE_VALUES, TARGET_VALUES, PREDICATE, INVERSE_PREDICATE) VALUES ( "+linkingId+", '"+sourceSubject+"', '"+targetSubject+"', '"+expression+"', '"+sourceValues+"', '"+targetValues+"', '"+predicate+"', '"+inversePredicate+"')";
		
		try {
			con = datasource.getConnection();
			pst = con.prepareStatement(query);
			pst.executeUpdate();
			
		} catch (SQLException ex) {
			System.out.println(">*>"+query);
			ex.printStackTrace();
		} finally {
			try {
				if (pst != null) {
					pst.close();
				}
				if (con != null) {
					con.close();
				}
				
			} catch (SQLException ex) {
				ex.toString();
			}
		}
	}
	
	public void existingRulesData() {
		Connection con = null;
		PreparedStatement pst = null;
		ResultSet rs = null;
		String query = "SELECT COUNT(ID) FROM LINKRULES;";
		try {
			con = datasource.getConnection();
			pst = con.prepareStatement(query);
			rs = pst.executeQuery();
			ExecutorService executor = Executors.newFixedThreadPool(HelioConfiguration.THREADS_LINKING_DATA);
			while (rs.next()) {
				System.out.println(">"+rs.getInt(1));
			}
			executor.shutdown();
		    executor.awaitTermination(HelioConfiguration.SYNCHRONOUS_TIMEOUT, HelioConfiguration.SYNCHRONOUS_TIMEOUT_TIME_UNIT);
		    executor.shutdownNow();
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			try {
				if (rs != null)
					rs.close();
				if (pst != null)
					pst.close();
				if (con != null)
					con.close();
			} catch (SQLException ex) {
				ex.toString();
			}
		}
	}
		
	public void linkData() {
		Connection con = null;
		PreparedStatement pst = null;
		ResultSet rs = null;
		String query = "SELECT DISTINCT A.LINKING_ID, A.SOURCE, B.TARGET, A.EXPRESSION, A.SOURCE_VALUES, B.TARGET_VALUES, A.PREDICATE, A.INVERSE_PREDICATE   FROM LINKRULES AS A INNER JOIN LINKRULES AS B WHERE A.LINKING_ID = B.LINKING_ID AND A.SOURCE != 'null' AND B.TARGET != 'null' ;";
		try {
			con = datasource.getConnection();
			pst = con.prepareStatement(query);
			rs = pst.executeQuery();
			ExecutorService executor = Executors.newFixedThreadPool(HelioConfiguration.THREADS_LINKING_DATA);
			while (rs.next()) {
				ExecutableLinkRule exLinkRule = new ExecutableLinkRule(rs.getInt(1), rs.getString(2), rs.getString(3),rs.getString(4),rs.getString(5),rs.getString(6), rs.getString(7), rs.getString(8));
				deleteLinkEntry(exLinkRule.getId()); 
				executor.submit( new Runnable() {
				    @Override
				    public void run() {
				    		exLinkRule.performLinking();
				
				    }
				});
			}
			executor.shutdown();
		    executor.awaitTermination(HelioConfiguration.SYNCHRONOUS_TIMEOUT, HelioConfiguration.SYNCHRONOUS_TIMEOUT_TIME_UNIT);
		    executor.shutdownNow();
		} catch (Exception ex) {
			logger.warn(ex.toString());;
		} finally {
			try {
				if (rs != null)
					rs.close();
				if (pst != null)
					pst.close();
				if (con != null)
					con.close();
			} catch (SQLException ex) {
				ex.toString();
			}
		}
	}
	
	
	private void deleteLinkEntry(Integer linkingId) {
		Connection con = null;
		PreparedStatement pst = null;
		String query = "DELETE FROM LINKRULES WHERE LINKING_ID = "+linkingId+" AND SOURCE != 'null' AND TARGET != 'null' ;";
		
		try {
			con = datasource.getConnection();
			pst = con.prepareStatement(query);
			pst.executeUpdate();
			
		} catch (SQLException ex) {
			System.out.println(">*>"+query);
			ex.printStackTrace();
		} finally {
			try {
				if (pst != null) {
					pst.close();
				}
				if (con != null) {
					con.close();
				}
				
			} catch (SQLException ex) {
				ex.toString();
			}
		}
	}

	// -- Expressions
	

	private static final String QUOTATION_STRING = "'";
	private static final String OPEN_DATA_REFERENCE_KEY = "{";
	private static final String CLOSE_DATA_REFERENCE_KEY = "}";

	@Override
	public String evaluateExpresions(String expression) {
		List<String> evaluableExpressions = retrieveExpressions(expression);
		
		// replace values in expression for the correct arguments in the function
		EvaluableExpression plainExpression = new EvaluableExpression(expression);
		List<String> dataReferences = plainExpression.getDataReferences();
		for(int index=0; index < dataReferences.size(); index++) {
			String dataReference = dataReferences.get(index);
			if(evaluableExpressions.isEmpty()) {
				expression = expression.replace(encloseArgument(dataReference,OPEN_DATA_REFERENCE_KEY,CLOSE_DATA_REFERENCE_KEY), dataReference);
			}else {
				expression = expression.replace(encloseArgument(dataReference,OPEN_DATA_REFERENCE_KEY,CLOSE_DATA_REFERENCE_KEY), encloseArgument(dataReference, QUOTATION_STRING,QUOTATION_STRING));
				
				//if(evaluableExpressions.stream().anyMatch(predicate -> predicate.contains(dataReference))) {
				//	}else {
				//	expression = expression.replace(encloseArgument(dataReference,OPEN_DATA_REFERENCE_KEY,CLOSE_DATA_REFERENCE_KEY), dataReference);
				//}
			}
		}
			
		if(!evaluableExpressions.isEmpty()) {
			evaluableExpressions = retrieveExpressions(expression);
			// evaluate the expressions
			for(int index=0; index < evaluableExpressions.size(); index++) {
				String expressionToEvaluate = evaluableExpressions.get(index);
				expression = expression.replace(encloseExpression(expressionToEvaluate), evalute(expressionToEvaluate));
			}			
		}
		
		return expression;
		
	}
	
	@Override
	public Boolean evaluatePredicate(String predicate) {
		return predicate(predicate);
	}
	
	
	
	private String evalute(String expression) {
		long startTime = System.nanoTime();
		List<String> result = new ArrayList<>();
		// 1. Evaluate expression in the H2 engine
		Connection connection = null;
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		ResultSetMetaData metadata = null;
		try {
			// 2. Get connection and prepare statement that executes the expression
			connection = datasource.getConnection();
			statement = connection.prepareStatement("CALL "+expression);
			// 3. Execute and retrieve statement results, which will be just one, the result of evaluating the expression
			resultSet = statement.executeQuery();
			metadata = resultSet.getMetaData();
		    int columnCount = metadata.getColumnCount(); 
			while (resultSet.next()) {
				for(int index=1; index <= columnCount; index++) {
					result.add(resultSet.getString(index));
				}
			}
		} catch (Exception exception1) {
			exception1.printStackTrace();
			logger.error(this.getClass()+": current expression contains errors "+expression);
		} finally {
			// 4. Close all opened objects
			try {
				if (resultSet != null)
					resultSet.close();
				if (statement != null)
					statement.close();
				if(connection != null)
					connection.close();
			} catch (SQLException exception2) {
				logger.error(exception2.toString());
			}
		}
		long stopTime = System.nanoTime();
		logger.info("Evaluating "+expression+" took: "+((stopTime - startTime) / 1000000) + " ms");
		return result.get(0); // TODO: CORRECT THIS?
	}
	
	private Boolean predicate(String predicate) {
		long startTime = System.nanoTime();
		Boolean evaluation = false;
		// 1. Evaluate expression in the H2 engine
		Connection connection = null;
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			// 2. Get connection and prepare statement that executes the expression
			connection = datasource.getConnection();
			statement = connection.prepareStatement("CALL " + predicate);
			// 3. Execute and retrieve statement results, which will be just one, the result of evaluating the predicate
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				evaluation = resultSet.getBoolean(1);
			}
		} catch (SQLException exception1) {
			logger.error(this.getClass()+": current predicate contains errors "+predicate);
		} finally {
			// 4. Close all opened objects
			try {
				if (resultSet != null)
					resultSet.close();
				if (statement != null)
					statement.close();
				if (connection != null)
					connection.close();
			} catch (SQLException exception2) {
				logger.error(exception2.toString());
			}
		}
		long stopTime = System.nanoTime();
		logger.info("Evaluating " + predicate + " took: " + ((stopTime - startTime) / 1000000) + " ms");
		
		return evaluation;
	}
	
	
		/**
		 * This method finds all the expressions embedded in a {@link String} value
		 * @param value A {@link String} value with expressions enclosed between characters '[' and ']'
		 * @return A list of Expressions allocated in the input {@link String} value
		 */
		public List<String> retrieveExpressions(String value){
			List<String> expressions = new ArrayList<>();
			// 0. Sometimes value may contain a reference with the expression special characters, for instance {[i13]}
			//  Therefore check first if exist any expression first
			String auxValue = value.replaceAll("\\{[^\\}]+\\}", "");
			if(auxValue.contains("[") && auxValue.contains("]")) {
				// 1. Compile pattern to find identifiers
				Pattern pattern = Pattern.compile("\\[([^\\[\\]]*|\\[[^\\[\\]]*\\])*\\]"); // to improve matching with this expression check https://stackoverflow.com/questions/17759004/how-to-match-string-within-parentheses-nested-in-java
				Matcher matcher = pattern.matcher(value);
				// 2. Find expressions with compiled matcher from the value
				while (matcher.find()) {
					String newIdentifier = matcher.group();
					// 2.1 Clean expression found
					newIdentifier = newIdentifier.replaceFirst("\\[", "");
					newIdentifier = replaceLast("]","", newIdentifier).trim();
					// 2.2 Add new clean expression found
					expressions.add(newIdentifier.replace("[", "&#91").replace("]", "&#93"));
				}
			}
			return expressions;
		}
		
		 public static String replaceLast(String find, String replace, String string) {
		        int lastIndex = string.lastIndexOf(find);
		        // Check precondition: 
		        if (lastIndex == -1) {
		            return string;
		        }
		        // Perform replacement
		        String beginString = string.substring(0, lastIndex);
		        String endString = string.substring(lastIndex + find.length());
		        // Aggregate different elements
		        StringBuilder result = new StringBuilder();
		        result.append(beginString);
		        result.append(replace);
		        result.append(endString);
		        
		        return result.toString();
		    }
		
		private String encloseExpression(String reference) {
			StringBuilder enclosedReference = new StringBuilder();
			enclosedReference.append("[").append(reference).append("]");
			return enclosedReference.toString();
		}
	
		private String encloseArgument(String reference, String value1, String value2) {
			StringBuilder enclosedReference = new StringBuilder();
			enclosedReference.append(value1).append(reference).append(value2);
			return enclosedReference.toString();
		}
		
		public void closeH2Cache() {
			Connection con = null;
			PreparedStatement pst = null;
			try {
				con = datasource.getConnection();
				pst = con.prepareStatement("DROP TABLE IF EXISTS LINKRULES;");
				pst.executeUpdate();
			} catch (SQLException ex) {
				ex.printStackTrace();
			} finally {
				try {
					if (pst != null) {
						pst.close();
					}
					if (con != null) {
						con.close();
					}
				
				} catch (SQLException ex) {
					ex.toString();
				}
			}
		}
	
	
}
