package helio.materialiser.data.handlers;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

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
	
	public JsonHandler(String iterator) {
		this.iterator = iterator;
	}
	
	public JsonHandler(JsonObject arguments) {
		if(arguments.has("iterator")) {
			iterator = arguments.get("iterator").getAsString();
			if(iterator.isEmpty())
				throw new IllegalArgumentException("JsonHandler needs to receive non empty value for the keey 'iterator'");
		}else {
			throw new IllegalArgumentException("JsonHandler needs to receive json object with the mandatory key 'iterator'");
		}
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
			
			List<Map<String,String>> results = JsonPath.using(conf).parse(dataStream).read(iterator);
			results.parallelStream().filter(map -> map != null && !map.isEmpty()).forEach(map -> queueOfresults.add(GSON.toJson(map)));
			try {
				dataStream.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return queueOfresults;
	}

	@Override
	public String filter(String filter, String dataChunk) {
		Configuration conf = Configuration.defaultConfiguration()
											.addOptions(Option.REQUIRE_PROPERTIES)
											.addOptions(Option.DEFAULT_PATH_LEAF_TO_NULL);
		
		
		return JsonPath.using(conf).parse(dataChunk).read(filter,String.class);
	}

	

}
