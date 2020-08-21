package helio.materialiser.configuration;

import java.util.concurrent.TimeUnit;

import org.eclipse.rdf4j.rio.RDFFormat;

public class HelioConfiguration {

	public static final Integer SYNCHRONOUS_TIMEOUT = 500;
	public static final TimeUnit SYNCHRONOUS_TIMEOUT_TIME_UNIT = TimeUnit.DAYS;
	public static final RDFFormat DEFAULT_RDF_FORMAT = RDFFormat.NTRIPLES;
	public static final String DEFAULT_BASE_URI = "http://helio.linkeddata.es/";
	
	// if this is false the synchronous data sources will be fired with a user request but will not block the user for consuming data (which can take more time and produce partial query results some times) 
	public static int THREADS_INJECTING_DATA = 100;
	public static int THREADS_HANDLING_DATA = 100;
	public static int THREADS_LINKING_DATA = 100;

	public static final String DEFAULT_DATA_INTERACTORS_PACKAGE = "helio.materialiser.data.*";
	
	public static final String DEFAULT_DATA_PROVIDER_PLUGINS_PACKAGE = "helio.materialiser.data.providers.";
	public static final String DEFAULT_DATA_HANDLER_PLUGINS_PACKAGE = "helio.materialiser.data.handlers.";
	public static final String PERSISTENT_CACHE_DIRECTORY = "./helio-cache";

	public static Object PLUGINS_FOLDER = "./plugins"; 
	
}
