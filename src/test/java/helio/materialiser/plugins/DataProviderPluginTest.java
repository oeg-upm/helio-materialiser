package helio.materialiser.plugins;

import java.io.InputStream;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Test;
import org.xeustechnologies.jcl.JarClassLoader;
import org.xeustechnologies.jcl.JclObjectFactory;

import helio.framework.exceptions.MalformedMappingException;
import helio.framework.materialiser.MappingTranslator;
import helio.framework.materialiser.mappings.DataProvider;
import helio.framework.materialiser.mappings.HelioMaterialiserMapping;
import helio.materialiser.HelioUtils;
import helio.materialiser.configuration.HelioConfiguration;
import helio.materialiser.data.PlugableClass;
import helio.materialiser.mappings.AutomaticTranslator;
import helio.materialiser.mappings.JsonTranslator;
import junit.framework.Assert;

public class DataProviderPluginTest {



	@Test
	public void testPlugableProvider() {
		JarClassLoader jcl = new JarClassLoader();
	  	 jcl.add("./src/test/resources/plugins/");
	  	 JclObjectFactory factory = JclObjectFactory.getInstance();
	  	 //Create object of loaded class
	  	 PluginDiscover.setPluginsDirectory("./src/test/resources/plugins/");
	  	Set<Class<? extends DataProvider>> classes =PluginDiscover.getDataProviderclasses();
	  	classes.stream().forEach( elem -> System.out.println(elem)); 
	   	Object obj = classes.stream().filter(clazz -> clazz.toString().endsWith(".PythonCodeProvider")).collect(Collectors.toList()).get(0);
	   	System.out.print(obj.toString());
	   	PlugableClass clazz = new PlugableClass(DataProvider.class, obj);
	  	InputStream in = clazz.getData();
	 //   Assert.assertTrue(transformToString(in).contains("{ \"key\": \"Ok\", \"number\": 32, \"text\" : \"Lorem ipsum dolor sit amet, consectetur adipiscing elit\" }"));
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
/*
	@Test
	 public void testPlugableProviderEth() throws MalformedMappingException {
			HelioConfiguration.PLUGINS_FOLDER = "./src/test/resources/plugins/";
			String mappingStr = HelioUtils.readFile("./src/test/resources/plugins/eth/test-plugin-2.json");
			MappingTranslator translator = new JsonTranslator();
			HelioMaterialiserMapping mapping = translator.translate(mappingStr);
			System.out.println(transformToString(mapping.getDatasources().get(0).getDataProvider().getData()));
	  	  	Assert.assertTrue(1==1);
		}*/
}
