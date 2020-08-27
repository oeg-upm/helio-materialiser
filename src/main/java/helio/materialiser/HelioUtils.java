package helio.materialiser;

import java.net.MalformedURLException;
import java.net.URL;

public class HelioUtils {

	private HelioUtils() {
		super();
	}
	
	public static String concatenate(String ... str) {
		StringBuilder builder = new StringBuilder();
		for(int index=0; index <str.length; index++) {
			builder.append(str[index]);
		}
		return builder.toString();
	}
	
	

	
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

	public static String createGraphIdentifier(String subject, String datasourceId) {
		StringBuilder builder = new StringBuilder();
		builder.append(subject).append("/").append(String.valueOf(datasourceId.hashCode()).replace("-", "0"));
		return builder.toString();
	}
	
	
}
