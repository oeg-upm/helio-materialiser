package helio.materialiser.run;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.jena.rdf.model.Model;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import helio.framework.materialiser.MappingTranslator;
import helio.framework.materialiser.mappings.HelioMaterialiserMapping;
import helio.materialiser.HelioMaterialiser;
import helio.materialiser.HelioUtils;
import helio.materialiser.configuration.HelioConfiguration;
import helio.materialiser.mappings.AutomaticTranslator;

public class Main {

	private static final String MAPPINGS_ARGUMENT = "--mappings=";
	private static final String CONFIG_ARGUMENT = "--config=";
	private static final String CLOSE_ARGUMENT = "--close";
	private static final String WRITE_ARGUMENT = "--write=";

	private static Logger logger = LogManager.getLogger(Main.class);

	
	private static final String MAN = "\n" + 
			"[Main command] java -jar materialiser-X.X.X.jar\n" + 
			"\n" + 
			"Usage:\n" + 
			"	--mappings= (mandatory): specifies the directory where the mappings file are located (bear in mind that Helio will put together all the mappings files creating a single one in memory)\n" + 
			"	--write= (optional): specifies a file in which the generated RDF will be written. This option can be used with the --cache flag.\n" + 
			"   --close (optional): specifies to Helio to shutdown the process after the data is generated, shutting down the asynchronous Data Sources.\n"+
			"   --config= (optional): specifies further Helio configurations.\n"+
			"	Any other flag will trigger this text.\n"
			+ "\n"
			+ "The result of the previous command either creates a dump file (using the --write) or injects the generated RDF data into the selected cache (using the --cache). Notice that the Helio command does not finish after is executed, the reason are the asynchronous Data Sources that will be updated when required, and therefore, keep the process live. In order to specify Helio to shutdown after the generation of RDF the argument --close must be used.\n";
	
	public static void main(String[] args) {
		List<String> arguments = Arrays.asList(args);
		if(contains(arguments,MAPPINGS_ARGUMENT)) {
			if(contains(arguments,CONFIG_ARGUMENT)) {
				System.out.println("Configuring Helio ...");
				String configurationFile = findArgument(CONFIG_ARGUMENT, arguments);
				HelioConfiguration.readConfigurationFile(configurationFile);
			}
			System.out.println("Reading mappings ...");
			String mappingsFolder = findArgument(MAPPINGS_ARGUMENT, arguments);
			HelioMaterialiserMapping mapping = readMappingsFolder(mappingsFolder);
			
			System.out.println("Generating data ...");
			HelioMaterialiser helio = new HelioMaterialiser(mapping);
			helio.updateSynchronousSources();
			
			System.out.println("Finished!");
			if(contains(arguments, WRITE_ARGUMENT)) {
				System.out.println("Writting data ..");
				String outputFile = findArgument(WRITE_ARGUMENT, arguments);	
				Model model = helio.getRDF();
				writeFile(outputFile, model);
			}
			if(contains(arguments,CLOSE_ARGUMENT)) {
				helio.close();
			}
		}else {
			System.out.println(MAN);
		}

	}
	
	private static boolean contains(List<String> arguments, String keyword) {
		Boolean contained =false;
		for(String argument:arguments) {
			if(argument.startsWith(keyword)) {
				contained = true;
				break;
			}
		}
		return contained;
	}

	private static void writeFile(String outputFile, Model model) {
		File targetFile = new File(outputFile);
	    try {
	    		if(targetFile.exists()) {
	    			targetFile.delete();
	    		}
	    		FileOutputStream out = new FileOutputStream(targetFile.getAbsoluteFile());
	    		try {
	    			model.write(out, "TTL", HelioConfiguration.DEFAULT_BASE_URI);
	    			}
	    			finally {
	    			  out.close();
	    			}
	    } catch (IOException e) {
	    		logger.error(e.toString());
		}
		
	}

	private static String findArgument(String key, List<String> arguments) {
		String argumentValue = null;
		for(String argument:arguments) {
			if(argument.startsWith(key)) {
				argumentValue = argument.replace(key, "");
				break;
			}
		}
		return argumentValue;
	}
	
	private static HelioMaterialiserMapping readMappingsFolder(String mappingsFolder) {
		HelioMaterialiserMapping mapping = new HelioMaterialiserMapping();
		MappingTranslator translator = new AutomaticTranslator();
		
		File mappingFolder = new File(mappingsFolder);
		File[] files = mappingFolder.listFiles();
		for(int index=0; index <files.length; index++) {
			File file = files[index];
			if(!file.isDirectory()) {
				String mappingStr = HelioUtils.readFile(file.getAbsolutePath());
				try {
					if(translator.isCompatible(mappingStr)) {	
						HelioMaterialiserMapping mappingAux = translator.translate(mappingStr);
						mapping.merge(mappingAux);
					}else {
						logger.warn("check the file: "+file);
					}
				}catch(Exception e) {
					logger.error("check the file: "+file);
				}
			}
		}
		return mapping;
	}
	


}
