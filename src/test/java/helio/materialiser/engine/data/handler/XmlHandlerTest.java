package helio.materialiser.engine.data.handler;

import java.io.File;
import java.util.List;
import java.util.Queue;

import org.junit.Assert;
import org.junit.Test;

import com.google.gson.JsonObject;

import helio.framework.materialiser.mappings.DataProvider;
import helio.materialiser.data.handlers.XmlHandler;
import helio.materialiser.data.providers.FileProvider;

public class XmlHandlerTest {

	

	@Test
	public void xmlTest() throws Exception {
		DataProvider fileProvider = new FileProvider(new File("./src/test/resources/handlers-tests/xml/test-xml.xml"));
		JsonObject object = new JsonObject();
		object.addProperty("iterator", "bookstore/book");
		XmlHandler handler = new XmlHandler();
		handler.configure(object);
		Queue<String> data = handler.splitData(fileProvider.getData());
		String polledData = data.poll();
		List<String> fragment = handler.filter("book//title", polledData);
		Assert.assertTrue(fragment.get(0).equals("Learning XML"));
		
		
	}
}
