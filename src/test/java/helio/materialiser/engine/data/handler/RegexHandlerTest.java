package helio.materialiser.engine.data.handler;

import java.io.File;
import java.util.Queue;

import org.junit.Assert;
import org.junit.Test;

import com.google.gson.JsonObject;

import helio.framework.materialiser.mappings.DataProvider;
import helio.materialiser.data.handlers.RegexHandler;
import helio.materialiser.data.providers.FileProvider;

public class RegexHandlerTest {

	
	@Test
	public void testText() {
		DataProvider fileProvider = new FileProvider(new File("./src/test/resources/test-csv-3.csv"));
		JsonObject configuration = new JsonObject();
		configuration.addProperty("iterator", "\"[^\"]+\"");
		
		RegexHandler handler = new RegexHandler();
		handler.configure(configuration);
		Queue<String> data = handler.splitData(fileProvider.getData());
		String getchedData = data.poll();
		String dataRetrieved = handler.filter("\\d+", getchedData).get(0);
		Assert.assertTrue(dataRetrieved.equals("1"));
	}
}
