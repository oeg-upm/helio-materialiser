package helio.materialiser.data.providers;

import java.io.InputStream;
import java.net.URL;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.google.gson.JsonObject;
import helio.framework.materialiser.mappings.DataProvider;

public class URLProvider implements DataProvider{

	private static final long serialVersionUID = 1L;
	private String resourceURL;
	private static Logger logger = LogManager.getLogger(URLProvider.class);

	@Override
	public InputStream getData() {
		InputStream output = null; 
		try {
			URL urlFile = new URL(resourceURL);
			output = urlFile.openStream();
		} catch (Exception e) {
			logger.error(e.toString());
		}
		return  output;
	}

	@Override
	public void configure(JsonObject configuration) {
		if(configuration.has("url")) {
			String resourceURLAux = configuration.get("url").getAsString();
			if(resourceURLAux.isEmpty()) {
				throw new IllegalArgumentException("URLProvider needs to receive non empty value for the key 'url'");
			}else{
				this.resourceURL = resourceURLAux;
			}
		}else {
			throw new IllegalArgumentException("URLProvider needs to receive json object with the mandatory key 'url'");
		}
	}

}
