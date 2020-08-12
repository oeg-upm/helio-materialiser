package helio.materialiser.run;

import java.io.InputStream;
import org.xeustechnologies.jcl.JarClassLoader;
import org.xeustechnologies.jcl.JclObjectFactory;
import helio.framework.materialiser.mappings.DataProvider;
import helio.materialiser.configuration.HelioConfiguration;

public class MyRecursiveAction {

    public static void main(String[] args) {
    	  JarClassLoader jcl = new JarClassLoader();
    	  jcl.add("./plugins");
    	  JclObjectFactory factory = JclObjectFactory.getInstance();
    	  //Create object of loaded class
    	  DataProvider obj = (DataProvider) factory.create(jcl, HelioConfiguration.DEFAULT_DATA_PROVIDER_PLUGINS_PACKAGE+".TestDataProviderPlugin");

    	  System.out.println(">"+transformToString(obj.getData()));
    	  
    }


    public static String transformToString(InputStream  input) {
		StringBuilder translatedStream = new StringBuilder();
		try {
			 int data = input.read();
			 while(data != -1){
					translatedStream.append((char) data);
		            data = input.read();
		            System.out.println(">"+translatedStream);
		            System.out.println(">"+data);
		        }
				System.out.println("read");
				input.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			
			
		}
		return translatedStream.toString();
	} 

}
