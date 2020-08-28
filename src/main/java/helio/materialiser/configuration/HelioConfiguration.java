package helio.materialiser.configuration;

import java.io.File;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.xeustechnologies.jcl.JarClassLoader;
import org.xeustechnologies.jcl.JclObjectFactory;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import helio.framework.materialiser.MaterialiserCache;
import helio.framework.materialiser.mappings.DataHandler;
import helio.framework.materialiser.mappings.DataProvider;
import helio.materialiser.HelioUtils;
import helio.materialiser.cache.RDF4JMemoryCache;
import helio.materialiser.evaluator.Functions;
import helio.materialiser.evaluator.H2Evaluator;

/**
 * This class contains all the configurable elements of the Helio materialiser software
 * @author Andrea Cimmino
 *
 */
public class HelioConfiguration {

	private static Logger logger = LogManager.getLogger(HelioConfiguration.class);

	
	private HelioConfiguration() {
		super();
	}
	
	// Final configuration
	
	/**
	 * Default format that Helio uses internally to represent the RDF data
	 */
	public static final String DEFAULT_RDF_FORMAT = "TTL";

	/**
	 * Default synchronous time out for threads
	 */
	public static final Integer SYNCHRONOUS_TIMEOUT = 500;
	/**
	 * Default synchronous time out units
	 */
	public static final TimeUnit SYNCHRONOUS_TIMEOUT_TIME_UNIT = TimeUnit.DAYS;
	
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
	 * Default package where the {@link Functions} are allocated, this package is where this software or the plugins projects MUST allocate their classes extending the {@link Functions} abstract class
	 */
	public static final String DEFAULT_FUNCTIONS_PLUGINS_PACKAGE = "helio.materialiser.evaluator.";
	/**
	 * Default package where the {@link MaterialiserCache} are allocated, this package is where this software or the plugins projects MUST allocate their {@link MaterialiserCache} implementations
	 */
	private static final String DEFAULT_MATERIALISER_CACHE_PLUGINS_PACKAGE = "helio.materialiser.cache.";

	// Dynamic configuration
	
	/**
	 * Default base URI used internally
	 */
	public static String DEFAULT_BASE_URI = "http://helio.linkeddata.es/";
	/**
	 * Default number of threads for injecting RDF triples
	 */
	public static Integer THREADS_INJECTING_DATA = 100;
	/**
	 * Default number of threads that translate chunks of data into RDF
	 */
	public static Integer THREADS_HANDLING_DATA = 100;
	
	/**
	 * Default number of threads for linking RDF triples
	 */
	public static Integer THREADS_LINKING_DATA = 100;

	/**
	 * Default folder that contains plugins
	 */
	public static String PLUGINS_FOLDER = "./plugins";
	
	/**
	 * Default folder to store the {@link MaterialiserCache} data 
	 */
	public static String DEFAULT_H2_PERSISTENT_CACHE_DIRECTORY = "./helio-cache";
	
	/**
	 * Default id of the {@link MaterialiserCache} 
	 */
	public static String DEFAULT_CACHE_ID = "helio-cache"; 
	
	/**
	 * The specific {@link MaterialiserCache} used by this software, it can be changed with any other object implementing the {@link MaterialiserCache} interface.
	 */
	public static MaterialiserCache HELIO_CACHE = new RDF4JMemoryCache(new File(HelioConfiguration.DEFAULT_H2_PERSISTENT_CACHE_DIRECTORY));
	/**
	 * The evaluator used to evaluate expressions, or perform the linking task.
	 */
	public static H2Evaluator EVALUATOR = new H2Evaluator();
	
	
	/**
	 * This method reads a file containing a Json document with all the available configurations, and sets up Helio with such configuration
	 * @param file the path of the file contaning the configuration
	 */
	public static void readConfigurationFile(String file) {
		String jsonStr = HelioUtils.readFile(file);
		if(jsonStr!=null &&  !jsonStr.isEmpty()) {
			try {
				Gson gson = new Gson();
				JsonObject jsonObject = gson.fromJson(jsonStr, JsonObject.class);
				 configure(jsonObject);
			}catch(Exception e) {
				logger.error(e.toString());
			}
		}else {
			logger.error("Provided configuration file could not be read, revise the file directory");
		}
	}

	/**
	 * This method parses the content of a Json document with the available configurations, and then, sets up Helio with such configuration
	 * @param jsonConfiguration a valid {@link JsonObject} containing the advanced configurtion
	 */
	public static void configure(JsonObject jsonConfiguration) {
		instantiateBaseURI(jsonConfiguration);
		instantiateThreads(jsonConfiguration);
		instantiatePluginsFolder(jsonConfiguration);
		instantiateCache(jsonConfiguration);
	}
	
	private static final String JSON_TOKEN_BASE_URI = "base_uri";
	private static final String JSON_TOKEN_PLUGINS = "plugins";
	private static final String JSON_TOKEN_PLUGINS_FOLDER = "folder";
	
	private static final String JSON_TOKEN_THREADS = "threads";
	private static final String JSON_TOKEN_THREADS_INJECTING_DATA = "injecting_data";
	private static final String JSON_TOKEN_THREADS_HANDLING_DATA= "splitting_data";
	private static final String JSON_TOKEN_THREADS_LINKING_DATA = "linking_data";
	
	private static final String JSON_TOKEN_REPOSITORY = "repository";
	private static final String JSON_TOKEN_REPOSITORY_ID = "id";
	private static final String JSON_TOKEN_REPOSITORY_CLASS = "type";
	private static final String JSON_TOKEN_CONFIGURATION_FILE = "configuration";
	
	private static void instantiateCache(JsonObject jsonObject) {
		if(jsonObject.has(JSON_TOKEN_REPOSITORY)) {
			JsonObject jsonObjectRespository = jsonObject.getAsJsonObject(JSON_TOKEN_REPOSITORY);
			if(jsonObjectRespository!=null && jsonObjectRespository.has(JSON_TOKEN_REPOSITORY_ID)) {
				DEFAULT_CACHE_ID = jsonObjectRespository.get(JSON_TOKEN_REPOSITORY_ID).getAsString();
				
			}else{
				logger.warn(HelioUtils.concatenate("New cache id was not specified in the configuration file (default is ",DEFAULT_CACHE_ID,")"));
			}
			if(jsonObjectRespository!=null && jsonObjectRespository.has(JSON_TOKEN_REPOSITORY_CLASS)) {
				String repositoryClass = jsonObjectRespository.get(JSON_TOKEN_REPOSITORY_CLASS).getAsString();
				instantiateNewReposiotry(repositoryClass);
			}else{
				logger.warn(HelioUtils.concatenate("New repository type was not specified in the configuration file (default is RDF4JMemoryCache persisting data in ",DEFAULT_H2_PERSISTENT_CACHE_DIRECTORY,")"));
			}
			if(jsonObjectRespository!=null && jsonObjectRespository.has(JSON_TOKEN_CONFIGURATION_FILE)) {
				String repositoryConfigurationFile = jsonObjectRespository.get(JSON_TOKEN_CONFIGURATION_FILE).getAsString();
				String configurationContent = HelioUtils.readFile(repositoryConfigurationFile);
				if(configurationContent!=null && !configurationContent.isEmpty()) {
					HELIO_CACHE.configureRepository(configurationContent);
				}else {
					logger.error("Configuration provided for the repository could not be read");
				}
			}else{
				logger.warn("New configuration file for the repository was provided");
			}
		}else{
			logger.warn(HelioUtils.concatenate("Repository configuration was not specified in the configuration file (default is RDF4JMemoryCache persisting data in ",DEFAULT_H2_PERSISTENT_CACHE_DIRECTORY,")"));
		}
	}

	private static void instantiateNewReposiotry(String repositoryClass) {
		JarClassLoader jcl = new JarClassLoader();
		jcl.add(HelioConfiguration.PLUGINS_FOLDER);
    		JclObjectFactory factory = JclObjectFactory.getInstance();
    		HELIO_CACHE = (MaterialiserCache) factory.create(jcl, HelioConfiguration.DEFAULT_MATERIALISER_CACHE_PLUGINS_PACKAGE+repositoryClass);
	}


	private static void instantiatePluginsFolder(JsonObject jsonObject) {
		if(jsonObject.has(JSON_TOKEN_PLUGINS)) {
			JsonObject pluginsJson = jsonObject.getAsJsonObject(JSON_TOKEN_PLUGINS);
			if(pluginsJson!=null && pluginsJson.has(JSON_TOKEN_PLUGINS_FOLDER)) {
				PLUGINS_FOLDER = pluginsJson.get(JSON_TOKEN_PLUGINS_FOLDER).getAsString();
			}else{
				logger.warn(HelioUtils.concatenate("plugins directory was not specified in the configuration file (default folder for plugins is ",PLUGINS_FOLDER,")"));
			}
		}else {
			logger.warn(HelioUtils.concatenate("Plugins configuration was not specified in the configuration file (default folder for plugins is ",PLUGINS_FOLDER,")"));
		}
	}
	
	private static void instantiateThreads(JsonObject jsonObject) {
		if(jsonObject.has(JSON_TOKEN_THREADS)) {
			JsonObject threadsJson = jsonObject.get(JSON_TOKEN_THREADS).getAsJsonObject();
			if(threadsJson!=null && threadsJson.has(JSON_TOKEN_THREADS_INJECTING_DATA)) {
				THREADS_INJECTING_DATA = threadsJson.get(JSON_TOKEN_THREADS_INJECTING_DATA).getAsInt();
			}else{
				logger.warn(HelioUtils.concatenate("The number of threads injecting data was not specified in the configuration file",THREADS_INJECTING_DATA.toString(),")"));
			}
			if(threadsJson!=null && threadsJson.has(JSON_TOKEN_THREADS_HANDLING_DATA)) {
				THREADS_HANDLING_DATA = threadsJson.get(JSON_TOKEN_THREADS_HANDLING_DATA).getAsInt();
			}else{
				logger.warn(HelioUtils.concatenate("The number of threads splitting data was not specified in the configuration file",THREADS_HANDLING_DATA.toString(),")"));
			}
			if(threadsJson!=null && threadsJson.has(JSON_TOKEN_THREADS_LINKING_DATA)) {
				THREADS_LINKING_DATA = threadsJson.get(JSON_TOKEN_THREADS_LINKING_DATA).getAsInt();
			}else{
				logger.warn(HelioUtils.concatenate("The number of threads linking data was not specified in the configuration file (default is ",THREADS_LINKING_DATA.toString(),")"));
			}
		}else{
			logger.warn(HelioUtils.concatenate("No threads configuration was provided in the configuration file (default is injecting=",THREADS_HANDLING_DATA.toString(),", splitting=",THREADS_HANDLING_DATA.toString(),", linking=",THREADS_LINKING_DATA.toString(),")"));
		}
		
		
	}

	private static void instantiateBaseURI(JsonObject jsonObject) {
		if(jsonObject.has(JSON_TOKEN_BASE_URI)) {
			DEFAULT_BASE_URI = jsonObject.get(JSON_TOKEN_BASE_URI).getAsString();
		}else{
			logger.warn(HelioUtils.concatenate("No base uri was provided in the configuration file (default is ", DEFAULT_BASE_URI, ")"));
		}
		
	}
	
}
