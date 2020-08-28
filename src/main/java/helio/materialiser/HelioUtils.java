package helio.materialiser;

import java.net.MalformedURLException;
import java.net.URL;

/**
 * This class provides a set of methods that ease the code writing 
 * @author Andrea Cimmino
 *
 */
public class HelioUtils {

	private HelioUtils() {
		super();
	}
	
	/**
	 * This method concatenates a set of strings efficiently in memory
	 * @param str a set of {@link String} values
 	 * @return a unique {@link String} concatenating all the input string values provided
	 */
	public static String concatenate(String ... str) {
		StringBuilder builder = new StringBuilder();
		for(int index=0; index <str.length; index++) {
			builder.append(str[index]);
		}
		return builder.toString();
	}
	
	/**
	 * This method checks whether a URL is correctly formed
	 * @param urlStr a set of {@link String} value containing the URL
	 * @return a boolean value that is true if the URL is correct, or false otherwise
	 */
	public static boolean isValidURL(String urlStr) {
	    try {
	    		if(urlStr == null || urlStr.contains(" ") || urlStr.isEmpty())
	    			throw new MalformedURLException();
	        new URL(urlStr);
	        return true;
	      }
	      catch (MalformedURLException e) {
	          return false;
	      }
	  }
	
	/**
	 * This method creates the a graph name for a subject taking into account its related {@link DataSource} id.<p>
	 * The graph identifier created should be used to name a graph that contains all the triples related to the subject.
	 * @param subject an RDF subject, i.e., a valid URI
	 * @param datasourceId the id of a {@link DataSource} 
	 * @return a valid URI that identifies the graph where all the triples related to a subject should be stored
	 */
	public static String createGraphIdentifier(String subject, String datasourceId) {
		StringBuilder builder = new StringBuilder();
		builder.append(subject).append("/").append(String.valueOf(datasourceId.hashCode()).replace("-", "0"));
		return builder.toString();
	}
	
	
}
