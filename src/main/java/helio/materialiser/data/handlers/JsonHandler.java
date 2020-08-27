package helio.materialiser.data.handlers;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;

import helio.framework.materialiser.mappings.DataHandler;

public class JsonHandler implements DataHandler {

	private static final long serialVersionUID = 1L;
	private static final Gson GSON = new Gson();
	private String iterator;
	private static Logger logger = LogManager.getLogger(JsonHandler.class);
	private static final String CONFIGURATION_KEY = "iterator";
	
	public JsonHandler() {
		super();
	}
	
	public JsonHandler(String iterator) {
		this.iterator = iterator;
	}
	
	public JsonHandler(JsonObject arguments) {
		configure(arguments);
	}
	
	public String getIterator() {
		return iterator;
	}

	public void setIterator(String iterator) {
		this.iterator = iterator;
	}

	@Override
	public Queue<String> splitData(InputStream dataStream) {
		ConcurrentLinkedQueue<String> queueOfresults = new ConcurrentLinkedQueue<>();
		if(dataStream!=null) {
			Configuration conf =  Configuration.defaultConfiguration()
												.addOptions(Option.ALWAYS_RETURN_LIST)
												.addOptions(Option.REQUIRE_PROPERTIES)
												.addOptions(Option.DEFAULT_PATH_LEAF_TO_NULL);
			try {
				List<Map<String,String>> results = JsonPath.using(conf).parse(dataStream).read(iterator);
				for(int index=0; index < results.size(); index++) {
					Map<String,String> map = results.get(index);
					if( map != null && !map.isEmpty())
						queueOfresults.add(GSON.toJson(map));
				}
				
				dataStream.close();
			} catch (Exception e) {
				logger.error(e.toString());
			}
		}
		return queueOfresults;
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<String> filter(String filter, String dataChunk) {
		
		Configuration conf = Configuration.defaultConfiguration()
											.addOptions(Option.REQUIRE_PROPERTIES)
											.addOptions(Option.DEFAULT_PATH_LEAF_TO_NULL);
		List<String> results = new ArrayList<>();
		try {
			Object parsed = JsonPath.using(conf).parse(dataChunk).read(filter);		
			if (parsed instanceof Collection) {
				results = (List<String>) parsed;
			}else {
				results.add(String.valueOf(parsed));
			}
		}catch(Exception e) {
			logger.error(e.toString());
		}
		
		return results;
	}

	@Override
	public void configure(JsonObject arguments) {
		if(arguments.has(CONFIGURATION_KEY)) {
			iterator = arguments.get(CONFIGURATION_KEY).getAsString();
			if(iterator.isEmpty())
				throw new IllegalArgumentException("JsonHandler needs to receive non empty value for the keey 'iterator'");
		}else {
			throw new IllegalArgumentException("JsonHandler needs to receive json object with the mandatory key 'iterator'");
		}
		
	}

	

}
