package helio.materialiser.configuration;

import java.util.concurrent.TimeUnit;

import org.eclipse.rdf4j.rio.RDFFormat;
import org.reflections.Configuration;

public class HelioConfiguration {

	public static final Integer SYNCHRONOUS_TIMEOUT = 500;
	public static final TimeUnit SYNCHRONOUS_TIMEOUT_TIME_UNIT = TimeUnit.DAYS;
	public static final RDFFormat DEFAULT_RDF_FORMAT = RDFFormat.TURTLE;
	public static final String DEFAULT_BASE_URI = "http://helio.linkeddata.es/";
	
	// if this is false the synchronous data sources will be fired with a user request but will not block the user for consuming data (which can take more time and produce partial query results some times) 
	public static final Boolean WAIT_FOR_SYNCHRONOUS_TRANSFORMATION_TO_FINISH = true;
	public static final String DEFAULT_DATA_HANDLERS_PACKAGE = "helio.materialiser.data.*"; 
	
}
