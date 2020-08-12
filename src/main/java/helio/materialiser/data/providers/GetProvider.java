package helio.materialiser.data.providers;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import helio.framework.materialiser.mappings.DataProvider;

public class GetProvider implements DataProvider {

	private static final long serialVersionUID = 1L;
	private String endpoint;
	private String method;
	private static Logger logger = LogManager.getLogger(URLProvider.class);
	private Map<String,String> headers;
	
	public GetProvider() {
		headers= new HashMap<>();
	}
	
	@Override
	public InputStream getData() {
		InputStream stream = null;
		try {
			URL url = new URL(endpoint);
			HttpURLConnection con = (HttpURLConnection) url.openConnection();
			con.setRequestMethod(method);
			headers.entrySet().stream().forEach(header -> con.setRequestProperty(header.getKey(), header.getValue()));
			// check https://www.baeldung.com/java-http-request for further advanced configurations: cookies, time outs, ...
			stream = con.getInputStream();
			
		} catch(Exception e) {
			logger.error(e.toString());
		}
		return stream;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void configure(JsonObject configuration) {
		if(configuration.has("url")) {
			String resourceURLAux = configuration.get("url").getAsString();
			if(resourceURLAux.isEmpty()) {
				throw new IllegalArgumentException("URLProvider needs to receive non empty value for the key 'url'");
			}else{
				this.endpoint = resourceURLAux;
			}
		}else {
			throw new IllegalArgumentException("URLProvider needs to receive json object with the mandatory key 'url'");
		}
		if(configuration.has("headers")) {
			JsonObject headersJson = configuration.get("headers").getAsJsonObject();
			Gson gson = new Gson();
			headers = gson.fromJson(headersJson, HashMap.class);
			
		}
	}

}
