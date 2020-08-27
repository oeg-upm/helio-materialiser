package helio.materialiser.plugins;

import java.io.InputStream;

import org.junit.Test;
import org.xeustechnologies.jcl.JarClassLoader;
import org.xeustechnologies.jcl.JclObjectFactory;

import helio.framework.materialiser.mappings.DataProvider;
import helio.materialiser.configuration.HelioConfiguration;
import junit.framework.Assert;

public class DataProviderPluginTest {

	
	
	@Test
	public void testPlugableProvider() {
		JarClassLoader jcl = new JarClassLoader();
	  	 jcl.add("./src/test/resources/plugins/");
	  	 JclObjectFactory factory = JclObjectFactory.getInstance();
	  	 //Create object of loaded class
	  	 DataProvider obj = (DataProvider) factory.create(jcl, HelioConfiguration.DEFAULT_DATA_PROVIDER_PLUGINS_PACKAGE+"TestDataProviderPlugin");

  	  	String outputData = transformToString(obj.getData());
  	  	Assert.assertTrue(outputData.contains("{ \"key\": \"Ok\", \"number\": 32, \"text\" : \"Lorem ipsum dolor sit amet, consectetur adipiscing elit\" }"));
	}
	
	 private String transformToString(InputStream  input) {
			StringBuilder translatedStream = new StringBuilder();
			try {
				 int data = input.read();
				 while(data != -1){
						translatedStream.append((char) data);
			            data = input.read();
			           
			        }
					input.close();
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				
				
			}
			return translatedStream.toString();
		} 
}
