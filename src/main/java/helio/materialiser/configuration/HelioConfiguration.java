package helio.materialiser.configuration;

import java.io.File;
import java.util.concurrent.TimeUnit;

import helio.framework.materialiser.MaterialiserCache;
import helio.materialiser.cache.RDF4JMemoryCache;
import helio.materialiser.evaluator.H2Evaluator;

/**
 * This class contains all the configurable elements of the Helio materialiser software
 * @author Andrea Cimmino
 *
 */
public class HelioConfiguration {

	private HelioConfiguration() {
		super();
	}
	/**
	 * Default format that Helio uses internally to represent the RDF data
	 */
	public static final String DEFAULT_RDF_FORMAT = "TTL";
	/**
	 * Default base URI used internally
	 */
	public static final String DEFAULT_BASE_URI = "http://helio.linkeddata.es/";
	/**
	 * Default synchronous time out for threads
	 */
	public static final Integer SYNCHRONOUS_TIMEOUT = 500;
	/**
	 * Default synchronous time out units
	 */
	public static final TimeUnit SYNCHRONOUS_TIMEOUT_TIME_UNIT = TimeUnit.DAYS;
	
	
	/**
	 * Default number of threads for injecting RDF triples
	 */
	public static int THREADS_INJECTING_DATA = 100;
	/**
	 * Default number of threads that translate chunks of data into RDF
	 */
	public static int THREADS_HANDLING_DATA = 100;
	
	/**
	 * Default number of threads for linking RDF triples
	 */
	public static int THREADS_LINKING_DATA = 100;

	/**
	 * Default package where the {@link DataProvider} and {@link DataHandler} are allocated
	 */
	public static final String DEFAULT_DATA_INTERACTORS_PACKAGE = "helio.materialiser.data.*";
	/**
	 * Default package where the {@link DataProvider} are allocated, this package is where this software or the plugins projects MUST allocate their {@link DataProvider} implementations
	 */
	public static final String DEFAULT_DATA_PROVIDER_PLUGINS_PACKAGE = "helio.materialiser.data.providers.";
	/**
	 * Default package where the {@link DataHandler} are allocated, this package is where this software or the plugins projects MUST allocate their {@link DataHandler} implementations
	 */
	public static final String DEFAULT_DATA_HANDLER_PLUGINS_PACKAGE = "helio.materialiser.data.handlers.";
	
	/**
	 * Default folder that contains plugins
	 */
	public static String PLUGINS_FOLDER = "./plugins";
	
	/**
	 * Default folder to store the {@link MaterialiserCache} data 
	 */
	public static final String PERSISTENT_CACHE_DIRECTORY = "./helio-cache";
	
	/**
	 * Default id of the {@link MaterialiserCache} 
	 */
	public static String DEFAULT_CACHE_ID = "helio-cache"; 
	
	
	/**
	 * The specific {@link MaterialiserCache} used by this software, it can be changed with any other object implementing the {@link MaterialiserCache} interface.
	 */
	public static final MaterialiserCache HELIO_CACHE = new RDF4JMemoryCache(new File(HelioConfiguration.PERSISTENT_CACHE_DIRECTORY));
	
	/**
	 * The evaluator used to evaluate expressions, or perform the linking task.
	 */
	public static final H2Evaluator EVALUATOR = new H2Evaluator();
	
	
	
	
}
