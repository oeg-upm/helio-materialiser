package helio.materialiser.evaluator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.lang.reflect.Modifier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.reflections.Reflections;
import java.lang.reflect.Method;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import helio.framework.expressions.EvaluableExpression;
import helio.framework.expressions.ExpressionsEvaluator;
import helio.framework.materialiser.mappings.LinkRule;
import helio.framework.objects.Utils;
import helio.materialiser.HelioUtils;
import helio.materialiser.configuration.HelioConfiguration;
import helio.materialiser.executors.ExecutableLinkRule;

/**
 * H2Evaluator allows to evaluate expressions, a composite of functions that can
 * return a {@link String} value as result or a {@link Boolean} value (a
 * predicate expression)
 * <p>
 * H2Evaluator is based on the H2 and Hikari database engine, @see <a href=
 * "http://www.h2database.com/html/functions.html#length">Documentation</a> link
 * for check the already implemented functions
 * <p>
 * To extend H2Evaluator functions a class must extend the abstract
 * {@link Functions} class, and then the method implementing the desired
 * functionality must be static. All the static function in such new class will
 * be registered in this evaluator and will be invokable
 * 
 * @author Andrea Cimmino
 *
 */
public class H2Evaluator implements ExpressionsEvaluator {

	// -- Attributes
	private HikariDataSource datasource;
	private static Logger logger = LogManager.getLogger(H2Evaluator.class);

	// -- Constructor
	/**
	 * Constructor of this class
	 */
	public H2Evaluator() {
		super();
		this.datasource = createInitHikari();
		loadProcedures();
	}

	// -- Methods

	/**
	 * This method initializes a Hikari datasource in-memory
	 * 
	 * @return A {@link HikariDataSource} object
	 */
	private static HikariDataSource createInitHikari() {
		HikariConfig config = new HikariConfig();
		config.setDataSourceClassName("org.h2.jdbcx.JdbcDataSource");
		config.setConnectionTestQuery("VALUES 1");
		config.setAutoCommit(true);
		config.setAllowPoolSuspension(true);
		config.addDataSourceProperty("URL", "jdbc:h2:file:./" + HelioConfiguration.DEFAULT_H2_PERSISTENT_CACHE_DIRECTORY
				+ "/helio-engine;MULTI_THREADED=TRUE;CACHE_SIZE=2048;DB_CLOSE_ON_EXIT=TRUE");
		// config.addDataSourceProperty("URL",
		// "jdbc:h2:mem:helio-engine-db;MULTI_THREADED=TRUE;CACHE_SIZE=2048;DB_CLOSE_ON_EXIT=TRUE");

		config.setMaximumPoolSize(50);
		return new HikariDataSource(config);
	}

	// methods to store functions

	/**
	 * This method register in the hikari datasource all the procedures implemented
	 * with java code that extends the {@link Function} abstract class
	 */
	private void loadProcedures() {
		// 1. Find all classes that extends Function
		Reflections reflections = new Reflections();
		Set<Class<? extends Functions>> classes = reflections.getSubTypesOf(Functions.class);
		for (Class<? extends Functions> clazz : classes) {
			Method[] methods = clazz.getMethods();
			for (Method method : methods) {
				if (Modifier.isStatic(method.getModifiers())) {
					// 2.1.1.A If the method is static register it in the datasource
					StringBuilder builder = new StringBuilder();
					builder.append("CREATE ALIAS IF NOT EXISTS ").append(method.getName());
					builder.append(" FOR \"").append(clazz.getName()).append(".").append(method.getName()).append("\"");
					logger.info("From " + clazz.getSimpleName() + " registering " + method.getName());
					registerFunction(builder.toString());
				}
			}
		}
		// 2. add all clases from plugins
		// TODO
	}

	/**
	 * This method registers a procedure creation statement into the datasource
	 * 
	 * @param function
	 *            A procedure creation statement
	 */
	private void registerFunction(String function) {
		try {
			Connection connection = datasource.getConnection();
			connection.setAutoCommit(true);
			PreparedStatement result = connection.prepareStatement(function);
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
			logger.warn(ex.toString());
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

	public void updateCache(String sourceSubject, String targetSubject, String sourceValues, String targetValues,
			LinkRule linkRule) {
		Connection con = null;
		PreparedStatement pst = null;
		String predicate = linkRule.getPredicate();
		String inversePredicate = linkRule.getInversePredicate();
		Integer linkingId = (linkRule.getSourceNamedGraph() + linkRule.getTargetNamedGraph()).hashCode();
		String expression = linkRule.getExpression().getExpression();
		String query = HelioUtils.concatenate(
				"INSERT INTO LINKRULES(LINKING_ID, SOURCE, TARGET, EXPRESSION, SOURCE_VALUES, TARGET_VALUES, PREDICATE, INVERSE_PREDICATE) VALUES ( ",
				linkingId.toString(), ", '", sourceSubject, "', '", targetSubject, "', '", expression, "', '",
				sourceValues, "', '", targetValues, "', '", predicate, "', '" + inversePredicate, "')");

		try {
			con = datasource.getConnection();
			pst = con.prepareStatement(query);
			pst.executeUpdate();

		} catch (SQLException ex) {
			logger.warn(ex.toString());
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
				ExecutableLinkRule exLinkRule = new ExecutableLinkRule(rs.getInt(1), rs.getString(2), rs.getString(3),
						rs.getString(4), rs.getString(5), rs.getString(6), rs.getString(7), rs.getString(8));
				deleteLinkEntry(exLinkRule.getId());
				executor.submit(new Runnable() {
					@Override
					public void run() {
						exLinkRule.performLinking();

					}
				});
			}
			executor.shutdown();
			executor.awaitTermination(HelioConfiguration.SYNCHRONOUS_TIMEOUT,
					HelioConfiguration.SYNCHRONOUS_TIMEOUT_TIME_UNIT);
			executor.shutdownNow();
		} catch (Exception ex) {
			logger.warn(ex.toString());
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
		try {
			con = datasource.getConnection();
			pst = con.prepareStatement(HelioUtils.concatenate("DELETE FROM LINKRULES WHERE LINKING_ID = ",
					linkingId.toString(), " AND SOURCE != 'null' AND TARGET != 'null' ;"));
			pst.executeUpdate();

		} catch (SQLException ex) {
			logger.warn(ex.toString());
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

	@Override
	public List<String> evaluateExpresions(EvaluableExpression expression) {
		List<String> expressions = new ArrayList<>();
		expressions.add(expression.getExpression());
		// fetch all the values
		List<String> functionReferences = expression.getFunctionReferences();
		for (int index = 0; index < functionReferences.size(); index++) {
			String functionReference = functionReferences.get(index);
			List<String> values = evaluate(functionReference);
			// Create as many expressions as values found
			expressions = expressions.parallelStream()
					.map(exp -> replaceFunctionReferences(exp, functionReference, values)).flatMap(Collection::stream)
					.collect(Collectors.toList());
		}

		return expressions;
	}
	
	private static final String CLOSING_FUNCTION_REFERENCE = "]";
	private static final String OPENING_FUNCTION_REFERENCE = "[";
	private List<String> replaceFunctionReferences(String expression, String reference, List<String> values) {
		String temporaryReference = Utils.buildMessage(OPENING_FUNCTION_REFERENCE,reference,CLOSING_FUNCTION_REFERENCE);
		return values.parallelStream().map(value -> expression.replace(temporaryReference, value)).collect(Collectors.toList());
	}

	@Override
	public Boolean evaluatePredicate(String predicate) {
		return predicate(predicate);
	}

	private List<String> evaluate(String expression) {
		long startTime = System.nanoTime();
		List<String> result = new ArrayList<>();

		// 1. Evaluate expression in the H2 engine
		try {
			// 2. Get connection and prepare statement that executes the expression
			try (Connection connection = datasource.getConnection()) {
				try (PreparedStatement statement = connection
						.prepareStatement(HelioUtils.concatenate("CALL ", expression))) {
					// 3. Execute and retrieve statement results, which will be just one, the result
					// of evaluating the expression
					try (ResultSet resultSet = statement.executeQuery()) {
						ResultSetMetaData metadata = resultSet.getMetaData();
						int columnCount = metadata.getColumnCount();
						while (resultSet.next()) {
							for (int index = 1; index <= columnCount; index++) {
								result.add(resultSet.getString(index));
							}
						}
					}
				}
			}
		} catch (Exception exception1) {
			logger.error(this.getClass() + ": current expression contains errors " + expression);
		}
		long stopTime = System.nanoTime();
		logger.debug("Evaluating " + expression + " took: " + ((stopTime - startTime) / 1000000) + " ms");
		return result;
	}

	private Boolean predicate(String predicate) {
		long startTime = System.nanoTime();
		Boolean evaluation = false;
		// 1. Evaluate expression in the H2 engine
		try {
			// 2. Get connection and prepare statement that executes the expression
			try (Connection connection = datasource.getConnection()) {
				try (PreparedStatement statement = connection.prepareStatement(Utils.buildMessage("CALL ", predicate))) {
					// 3. Execute and retrieve statement results, which will be just one, the result
					// of evaluating the predicate
					try (ResultSet resultSet = statement.executeQuery()) {
						while (resultSet.next()) {
							evaluation = resultSet.getBoolean(1);
						}
					}
				}
			}
		} catch (SQLException exception1) {
			logger.error(this.getClass() + ": current predicate contains errors " + predicate);
		}
		long stopTime = System.nanoTime();
		logger.info("Evaluating " + predicate + " took: " + ((stopTime - startTime) / 1000000) + " ms");

		return evaluation;
	}

	public void closeH2Cache() {
		Connection con = null;
		PreparedStatement pst = null;
		try {
			con = datasource.getConnection();
			pst = con.prepareStatement("DELETE FROM  LINKRULES;");
			pst.executeUpdate();
		} catch (SQLException ex) {
			logger.warn(ex.toString());
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
