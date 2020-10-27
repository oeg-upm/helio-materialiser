package helio.materialiser.plugins;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import helio.framework.materialiser.MappingTranslator;
import helio.framework.materialiser.MaterialiserCache;
import helio.framework.materialiser.mappings.DataHandler;
import helio.framework.materialiser.mappings.DataProvider;
import helio.materialiser.HelioUtils;
import helio.materialiser.evaluator.Functions;

public class Plugins {

	private static Map<String,String> dataProviderPlugins = new HashMap<>();
	private static Map<String,String> dataHandlerPlugins = new HashMap<>();
	private static Map<String,String> functionsPlugins = new HashMap<>();
	private static Map<String,String> translatorPlugins = new HashMap<>();
	private static Map<String,String> cachePlugins = new HashMap<>();
	
	private static Logger logger = LogManager.getLogger(Plugins.class);
	private static final String JSON_TOKEN_PLUGINS = "plugins";
	private static final String JSON_TOKEN_PLUGINS_SOURCE= "source";
	private static final String JSON_TOKEN_PLUGINS_CLASS = "class";
	private static final String JSON_TOKEN_PLUGINS_CLASS_TYPE = "type";
	private static final List<String> JSON_TOKEN_PLUGINS_CLASS_TYPE_AVAILABLE = new ArrayList<>(); 
	static {
		JSON_TOKEN_PLUGINS_CLASS_TYPE_AVAILABLE.add("DataProvider");
		JSON_TOKEN_PLUGINS_CLASS_TYPE_AVAILABLE.add("DataHandler");
		JSON_TOKEN_PLUGINS_CLASS_TYPE_AVAILABLE.add("HelioCache");
		JSON_TOKEN_PLUGINS_CLASS_TYPE_AVAILABLE.add("Functions");
		JSON_TOKEN_PLUGINS_CLASS_TYPE_AVAILABLE.add("MappingTranslator");
	}
	
	private Plugins() {
		super();
	}
	
	// Load Methods
	
	public static void loadPluginsFromJsonConfiguration(JsonObject jsonObject) {
		if(jsonObject.has(JSON_TOKEN_PLUGINS)) {
			JsonArray jsonArray = jsonObject.getAsJsonArray(JSON_TOKEN_PLUGINS);
			for(int index=0; index< jsonArray.size(); index++) {
				JsonObject pluginModule = jsonArray.get(index).getAsJsonObject();
				loadPluginsFromJsonModule(pluginModule);	
			}
		}else {
			logger.warn(HelioUtils.concatenate("Plugins configuration was not specified in the configuration file using key 'plugins'"));
		}
		
	}
	
	public static void loadPluginsFromJsonModule(JsonObject pluginModule) {
		if(pluginModule.has(JSON_TOKEN_PLUGINS_SOURCE) && pluginModule.has(JSON_TOKEN_PLUGINS_CLASS) && pluginModule.has(JSON_TOKEN_PLUGINS_CLASS_TYPE)) {
			String type = pluginModule.get(JSON_TOKEN_PLUGINS_CLASS_TYPE).getAsString();
			String source = pluginModule.get(JSON_TOKEN_PLUGINS_SOURCE).getAsString();
			String clazz = pluginModule.get(JSON_TOKEN_PLUGINS_CLASS).getAsString();
			if(!type.isEmpty() && !source.isEmpty() && !clazz.isEmpty()) {
				if(JSON_TOKEN_PLUGINS_CLASS_TYPE_AVAILABLE.contains(type)) {
					loadPluginPointer( source,  clazz,  type) ;
				}else {
					logger.error(HelioUtils.concatenate("Provided plugin type is not supported, supported plugin types are:",JSON_TOKEN_PLUGINS_CLASS_TYPE_AVAILABLE.toString(),". Provided one was: ",type));
				}
			}else {
				logger.error(HelioUtils.concatenate("No empty values allowed for the keys 'source', 'class', 'type'"));

			}
		}else{
			logger.error(HelioUtils.concatenate("Provided json is missing mandatory keys, keys must be: 'source', 'class', 'type'. Instead provided json has ",pluginModule.keySet().toString()));
		}
	}
	
	private static void loadPluginPointer(String source, String clazz, String type) {
		if(type.equals("DataProvider"))
			dataProviderPlugins.put(clazz, source);
		if(type.equals("DataHandler"))
			dataHandlerPlugins.put(clazz, source);
		if(type.equals("Functions"))
			functionsPlugins.put(clazz, source);
		if(type.equals("HelioCache"))
			cachePlugins.put(clazz, source);
		if(type.equals("MappingTranslator"))
			translatorPlugins.put(clazz, source);
	
	}
	
	// Build methods
	
	
	public static DataHandler buildDataHandlerByName(String className) {
		DataHandler dataHandlerPlugin = null;
		Optional<Entry<String,String>> entryFoundOpt = dataHandlerPlugins.entrySet().stream().filter(entry-> entry.getKey().endsWith("."+className)).findFirst();
		if(entryFoundOpt.isPresent()) {
			Entry<String,String> entryFound = entryFoundOpt.get();
			dataHandlerPlugin = buildDataHandler(entryFound.getValue(), entryFound.getKey());
		}else {
			logger.error("Requested class is not present in the available data handler plugins, which are : ", dataHandlerPlugins.values().toString());
		}
		return dataHandlerPlugin;
	}
	
	public static DataHandler buildDataHandler(String source, String clazz) {
		DataHandler dataHandlerPlugin = null;
		try {
			ExtensionLoader<DataHandler> loader = new ExtensionLoader<>();
			dataHandlerPlugin = loader.loadClass(source, clazz, DataHandler.class);
		}catch(Exception e){
			logger.error(e.toString());
		}
		return dataHandlerPlugin;
	}
	
	public static DataProvider buildDataProviderByName(String className) {
		DataProvider dataProviderPlugin = null;
		Optional<Entry<String,String>> entryFoundOpt = Plugins.dataProviderPlugins.entrySet().stream().filter(entry-> entry.getKey().endsWith("."+className)).findFirst();
		if(entryFoundOpt.isPresent()) {
			Entry<String,String> entryFound = entryFoundOpt.get();
			dataProviderPlugin = buildDataProvider(entryFound.getValue(), entryFound.getKey());
		}else {
			logger.error("Requested class is not present in the available data provider plugins, which are : ", dataHandlerPlugins.values().toString());
		}
		return dataProviderPlugin;
	}
	
	public static DataProvider buildDataProvider(String source, String clazz) {
		DataProvider dataProviderPlugin = null;
		try {
			ExtensionLoader<DataProvider> loader = new ExtensionLoader<>();
			dataProviderPlugin = loader.loadClass(source, clazz, DataProvider.class);
		}catch(Exception e){
			logger.error(e.toString());
		}
		return dataProviderPlugin;
	}
	
	public static Set<Functions> buildLoadedFunctions(){
		Set<Functions> functions = new HashSet<>();
		for(Entry<String,String> entry:functionsPlugins.entrySet()){
			Functions translator = buildFunctions(entry.getValue(), entry.getKey());
			functions.add(translator);
		}
		return functions;
	}
	
	
	public static Functions buildFunctionsByName(String className) {
		Functions functionsPlugin = null;
		Optional<Entry<String,String>> entryFoundOpt = functionsPlugins.entrySet().stream().filter(entry-> entry.getKey().endsWith("."+className)).findFirst();
		if(entryFoundOpt.isPresent()) {
			Entry<String,String> entryFound = entryFoundOpt.get();
			functionsPlugin = buildFunctions(entryFound.getValue(), entryFound.getKey());
		}else {
			logger.error("Requested class is not present in the available function plugins, which are : ", dataHandlerPlugins.values().toString());
		}
		return functionsPlugin;
	}
	
	public static Functions buildFunctions(String source, String clazz) {
		Functions functionsPlugins = null;
		try {
			ExtensionLoader<Functions> loader = new ExtensionLoader<>();
			functionsPlugins = loader.loadClass(source, clazz, Functions.class);
		}catch(Exception e){
			logger.error(e.toString());
		}
		return functionsPlugins;
	}
	
	public static MaterialiserCache buildMaterialiserCacheByName(String className) {
		MaterialiserCache cachePlugin = null; // check this functions
		Optional<Entry<String,String>> entryFoundOpt = cachePlugins.entrySet().stream().filter(entry-> entry.getKey().endsWith("."+className)).findFirst();
		if(entryFoundOpt.isPresent()) {
			Entry<String,String> entryFound = entryFoundOpt.get();
			cachePlugin = buildCache(entryFound.getValue(), entryFound.getKey());
		}else {
			logger.error("Requested class is not present in the available cache plugins, which are : ", dataHandlerPlugins.values().toString());
		}
		return cachePlugin;
	}
	
	
	public static MaterialiserCache buildCache(String source, String clazz) {
		MaterialiserCache materialiserCachePlugins = null;
		try {
			ExtensionLoader<MaterialiserCache> loader = new ExtensionLoader<>();
			materialiserCachePlugins = loader.loadClass(source, clazz, MaterialiserCache.class);
		}catch(Exception e){
			logger.error(e.toString());
		}
		return materialiserCachePlugins;
	}
	
	
	public static Set<MappingTranslator> buildLoadedMappingTranslator(){
		Set<MappingTranslator> translators = new HashSet<>();
		for(Entry<String,String> entry:translatorPlugins.entrySet()){
			MappingTranslator translator = buildTranslator(entry.getValue(), entry.getKey());
			translators.add(translator);
		}
		return translators;
	}
	
	public static MappingTranslator buildMappingTranslatorByName(String className) {
		MappingTranslator translatorPlugin = null; // check this functions
		Optional<Entry<String,String>> entryFoundOpt = translatorPlugins.entrySet().stream().filter(entry-> entry.getKey().endsWith("."+className)).findFirst();
		if(entryFoundOpt.isPresent()) {
			Entry<String,String> entryFound = entryFoundOpt.get();
			translatorPlugin = buildTranslator(entryFound.getValue(), entryFound.getKey());
		}else {
			logger.error("Requested class is not present in the available mapping translator plugins, which are : ", dataHandlerPlugins.values().toString());
		}
		return translatorPlugin;
	}
	
	public static MappingTranslator buildTranslator(String source, String clazz) {
		MappingTranslator materialiserTranslatorPlugins = null;
		try {
			ExtensionLoader<MappingTranslator> loader = new ExtensionLoader<>();
			materialiserTranslatorPlugins = loader.loadClass(source, clazz, MappingTranslator.class);
		}catch(Exception e){
			logger.error(e.toString());
		}
		return materialiserTranslatorPlugins;
	}
	
	
}
