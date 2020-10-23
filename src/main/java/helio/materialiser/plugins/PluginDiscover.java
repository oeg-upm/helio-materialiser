package helio.materialiser.plugins;

import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


import helio.framework.materialiser.MaterialiserCache;
import helio.framework.materialiser.mappings.DataHandler;
import helio.framework.materialiser.mappings.DataProvider;
import helio.materialiser.HelioUtils;
import helio.materialiser.evaluator.Functions;
import helio.materialiser.mappings.AutomaticTranslator;

public class PluginDiscover {

	private static Logger logger = LogManager.getLogger(PluginDiscover.class);
	public static PlugINt plugint = new PlugINt();
	private static Set<Class<? extends DataProvider>> dataProviderClasses;
	private static  Set<Class<? extends DataHandler>> dataHandlerClasses;
	private static  Set<Class<? extends MaterialiserCache>> cacheClasses;
	private static  Set<Class<? extends Functions>> functionsClasses;
	private static String pluginsDirectory = null;
	private static String repository = null;
	
	static {
	   dataProviderClasses = new HashSet<>();
	   dataHandlerClasses = new HashSet<>();
	   cacheClasses = new HashSet<>();
	   functionsClasses = new HashSet<>();
	  }
	 
	// -- Getters and Setters
	
	public static String getPluginsDirectory() {
		return pluginsDirectory;
	}
	
	public static void setPluginsDirectory(String directory) {
		if(directory.startsWith("./"))
			directory = directory.replace("./", "");
		pluginsDirectory = directory;			
	    loadPluginFromFile();
	}
	
	public static String getPluginsRepository() {
		return repository;
	}
	
	public static void setPluginsRepository(String repositoryURL) {
		repository = repositoryURL;
		loadPluginFromRepository();
	}
	
	// -- Plugins loaders
	
	private static void loadPluginFromRepository() {
		if(repository!=null) {
			logger.info("Reading plugins from: "+repository);
			// TODO:
		}
	}
	
	private static void loadPluginFromFile() {
		if(pluginsDirectory!=null && !pluginsDirectory.isEmpty()) {
			String logEntry = HelioUtils.concatenate("Reading plugins from: ",pluginsDirectory);
			logger.info(logEntry);
			plugint.loadJarFromDirectory(pluginsDirectory);
			File pluginsFolder = new File(pluginsDirectory);
			if(pluginsFolder.exists()) {
				// Load all connector objects from plugins folder
				try {
					List<Class<?>> connectors = plugint.createClassesByInterfaceFullName(DataProvider.class.getName());
					connectors.stream().forEach(object -> dataProviderClasses.add((Class<? extends DataProvider>) object));
				}catch(Exception e) {
					
				}
				try {
				// Load all datasources objects from plugins folder
				List<Class<?>> datasouces = plugint.createClassesByInterfaceFullName(DataHandler.class.getName());
				datasouces.stream().forEach(object -> dataHandlerClasses.add((Class<? extends DataHandler>) object));
				}catch(Exception e) {
					
				}
				try {
				// Load all translator objects from plugins folder
					List<Class<?>> mappingTranslator = plugint.createClassesByInterfaceFullName(MaterialiserCache.class.getName());
					mappingTranslator.stream().forEach(object -> cacheClasses.add((Class<? extends MaterialiserCache>) object));
				}catch(Exception e) {
					
				}
				// Load all functions objects from plugins folder
				//List<Class<?>> functions = plugint.createClassesByInterfaceFullName(Functions.class.getName());
				//functions.stream().forEach(object -> functionsClasses.add((Class<? extends Functions>) object));
				// Create logs entries
				createLogEntries();
			}else {
				logger.error("Provided plugin folder does not exists");
			}
		}
	}
	
	


	private static void createLogEntries() {
		String logEntry = HelioUtils.concatenate("Connectors loaded from plugins: ", String.valueOf(dataProviderClasses.size()));
		logger.info(logEntry);
		logEntry = HelioUtils.concatenate("Datasources loaded from plugins: ", String.valueOf(dataHandlerClasses.size()));
		logger.info(logEntry);
		logEntry = HelioUtils.concatenate("MappingTranslators loaded from plugins: ", String.valueOf(cacheClasses.size()));
		logger.info(logEntry);
		logEntry = HelioUtils.concatenate("Functions loaded from plugins: ", String.valueOf(functionsClasses.size()));
		logger.info(logEntry);
	}
	
	// Getters


	public static Set<Class<? extends DataProvider>> getDataProviderclasses() {
		return dataProviderClasses;
	}


	public static Set<Class<? extends DataHandler>> getDatasHandlerclasses() {
		return dataHandlerClasses;
	}


	public static Set<Class<? extends MaterialiserCache>> getCachesclasses() {
		return cacheClasses;
	}

	public static Set<Class<? extends Functions>> getFunctionsclasses() {
		return functionsClasses;
	}
	
}
