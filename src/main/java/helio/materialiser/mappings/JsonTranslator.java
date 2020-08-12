package helio.materialiser.mappings;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.reflections.Reflections;
import org.xeustechnologies.jcl.JarClassLoader;
import org.xeustechnologies.jcl.JclObjectFactory;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import helio.framework.exceptions.MalformedMappingException;
import helio.framework.materialiser.MappingTranslator;
import helio.framework.materialiser.mappings.DataHandler;
import helio.framework.materialiser.mappings.DataProvider;
import helio.framework.materialiser.mappings.DataSource;
import helio.framework.materialiser.mappings.EvaluableExpression;
import helio.framework.materialiser.mappings.HelioMaterialiserMapping;
import helio.framework.materialiser.mappings.Rule;
import helio.framework.materialiser.mappings.RuleSet;
import helio.materialiser.configuration.HelioConfiguration;


public class JsonTranslator implements MappingTranslator{

	private static Logger logger = LogManager.getLogger(JsonTranslator.class);
	private static final String DATASOURCES_TOKEN = "datasources";
	private static final String RULES_TOKEN = "resource_rules";
	private static final String DATA_PROVIDER_TOKEN = "provider";
	private static final String DATA_HANDLER_TOKEN = "handler";
	private static final String DATA_SOURCE_ID_TOKEN = "id";
	private static final String DATA_SOURCE_REFRESH_TOKEN = "refresh";
	
	private JarClassLoader jcl;
	private JclObjectFactory factory;
	
	public JsonTranslator() {
		jcl = new JarClassLoader();
	    	jcl.add(HelioConfiguration.PLUGINS_FOLDER);
	    	factory = JclObjectFactory.getInstance();
	}
	
	
	@Override
	public HelioMaterialiserMapping translate(String mappingContent) throws MalformedMappingException {
		HelioMaterialiserMapping mapping = new HelioMaterialiserMapping();
		try {
			JsonObject json = (new Gson()).fromJson(mappingContent, JsonObject.class);
			try {
				// add parsed DataSources
				List<DataSource> datasources = parseDataSources(json);
				mapping.getDatasources().addAll(datasources);
			}catch(Exception e) {
				logger.error(e.toString());
			}
			try {
				// add parsed ResourceRules
				List<RuleSet> ruleSets = parseRuleSets(json);
				mapping.getRuleSets().addAll(ruleSets);
			}catch(Exception e) {
				logger.error(e.toString());
			}
			// TODO: add parsed Linking rules
		}catch(Exception e) {
			logger.error(e.toString());
		}
		return mapping;
	}
	
	// -- Data Source methods
	
	private List<DataSource> parseDataSources(JsonObject json) throws MalformedMappingException{
		List<DataSource> dss = new ArrayList<>();
		if(json.has(DATASOURCES_TOKEN)) {
			JsonArray dataSourceArray = json.getAsJsonArray(DATASOURCES_TOKEN);
			for(int index=0; index < dataSourceArray.size(); index++) {
				JsonObject dataSourceJson = dataSourceArray.get(index).getAsJsonObject();
				DataSource dataSource = initialiseDataSource(dataSourceJson);
				dss.add(dataSource);
			}
		}else {
			logger.warn("Provided mapping has no data sources defined");
		}
		return dss;
	}
	
	
	
	private DataSource initialiseDataSource(JsonObject dataSourceJson) throws MalformedMappingException {
		DataSource datasource = new DataSource();
		if(dataSourceJson.has(DATA_PROVIDER_TOKEN) && dataSourceJson.has(DATA_HANDLER_TOKEN)) {
			String datasourceId = dataSourceJson.get(DATA_SOURCE_ID_TOKEN).getAsString();
			Integer refreshTime = null;
			if(dataSourceJson.has(DATA_SOURCE_REFRESH_TOKEN))
				refreshTime = dataSourceJson.get(DATA_SOURCE_REFRESH_TOKEN).getAsInt();
			DataHandler dataHandler = initialiseDataHandler(dataSourceJson.get(DATA_HANDLER_TOKEN).getAsJsonObject());
			DataProvider dataProvider = initialiseDataProvider(dataSourceJson.get(DATA_PROVIDER_TOKEN).getAsJsonObject());
			datasource.setId(datasourceId);
			datasource.setRefresh(refreshTime);
			datasource.setDataHandler(dataHandler);
			datasource.setDataProvider(dataProvider);
			
		}else {
			throw new MalformedMappingException("provided mapping is missing mandatory keys, either the data source or the data provider");
		}
		return datasource;
	}

	private DataHandler initialiseDataHandler(JsonObject jsonObject) throws MalformedMappingException {
		DataHandler dataHandler = null;
		// 0. Retrieve all datasource classes in class path
		Reflections reflections = new Reflections(HelioConfiguration.DEFAULT_DATA_HANDLERS_PACKAGE);    
		Set<Class<? extends DataHandler>> dataHandlerClasses = reflections.getSubTypesOf(DataHandler.class);	
		try {
			// 1. Retrieve type
			String dataHandlerClassName = jsonObject.get("type").getAsString();	
			jsonObject.remove("type");
			// 3. Find in the class path the package that contains the datasourceClassName
			Optional<Class<? extends DataHandler>> dataHandlerClassOptional = dataHandlerClasses.stream().filter(datasourceClazz -> datasourceClazz.getName().endsWith("."+dataHandlerClassName)).findFirst();
			// 3. Create datasource using its class name
			if(dataHandlerClassOptional.isPresent()) {
				Class<? extends DataHandler> dataHandlerClass = dataHandlerClassOptional.get();
				dataHandler = dataHandlerClass.getConstructor(JsonObject.class).newInstance(jsonObject);
			}else {
				throw new MalformedMappingException(" specified data handler does not exists: "+dataHandlerClassName);
			}
		} catch(Exception e) {
			e.printStackTrace();
			throw new MalformedMappingException("an error happened instantiating the data handler, please review the mappings");
		}
		return dataHandler;
	}

	private DataProvider initialiseDataProvider(JsonObject jsonObject) throws MalformedMappingException {
		DataProvider dataProvider = null;
		// 0. Retrieve all datasource classes in class path
		Reflections reflections = new Reflections(HelioConfiguration.DEFAULT_DATA_HANDLERS_PACKAGE);
		Set<Class<? extends DataProvider>> dataProviderClasses = reflections.getSubTypesOf(DataProvider.class);
		try {
			// 1. Retrieve type
			String dataProviderClassName = jsonObject.get("type").getAsString();
			jsonObject.remove("type");
			// 3. Find in the class path the package that contains the datasourceClassName
			Optional<Class<? extends DataProvider>> dataProviderClassOptional = dataProviderClasses.stream()
					.filter(dataProviderClazz -> dataProviderClazz.getName().endsWith("." + dataProviderClassName))
					.findFirst();
			// 3. Create datasource using its class name
			if (dataProviderClassOptional.isPresent()) {
				Class<? extends DataProvider> dataProviderClass = dataProviderClassOptional.get();
				dataProvider = dataProviderClass.getConstructor().newInstance();
			} else {
				// 3.1 try to find the provider in the plugins
				dataProvider = (DataProvider) instantiateObjectFromPlugins(HelioConfiguration.DEFAULT_DATA_HANDLERS_PACKAGE, dataProviderClassName);
			}
			if(dataProvider ==null) {
				throw new MalformedMappingException(" specified data provider does not exists: " + dataProviderClassName);
			}else {
				dataProvider.configure(jsonObject);
			}
		} catch (Exception e) {
			throw new MalformedMappingException(
					"an error happened instantiating the data provider, please review the mappings");
		}
		return dataProvider;
	}
	
	private Object instantiateObjectFromPlugins(String packageClass, String className) {
		Object newObject = null;
		try {
			newObject = factory.create(jcl, "helio.materialiser.data.providers.");
		}catch(Exception e) {
			logger.warn("Class "+className+" not found among the plugins");
			logger.info("Make sure class is allocated in the correct package (it should be "+packageClass+")");
		}
		return newObject;
	}
	

	// -- Rule sets methods
	
	private List<RuleSet> parseRuleSets(JsonObject json) throws MalformedMappingException{
		List<RuleSet> rss = new ArrayList<>();
		if(json.has(RULES_TOKEN)) {
			JsonArray ruleSetArray = json.getAsJsonArray(RULES_TOKEN);
			for(int index=0; index < ruleSetArray.size(); index++) {
				JsonObject ruleSetJson = ruleSetArray.get(index).getAsJsonObject();
				RuleSet ruleSet = initialiseRuleSet(ruleSetJson);
				rss.add(ruleSet);
			}
		}else {
			logger.warn("Provided mapping has no data resource rules defined");
		}
		return rss;
	}
	

	private RuleSet initialiseRuleSet(JsonObject ruleSetJson) throws MalformedMappingException {
		RuleSet ruleSet = new RuleSet();
		if(ruleSetJson.has("id") && ruleSetJson.has("datasource_ids") && ruleSetJson.has("subject") && ruleSetJson.has("properties")) {
			String resourceRuleId = ruleSetJson.get("id").getAsString();
			ruleSet.setResourceRuleId(resourceRuleId);
			ruleSetJson.get("datasource_ids").getAsJsonArray().forEach(ds-> ruleSet.getDatasourcesId().add(ds.getAsString()));
			String strSubject = ruleSetJson.get("subject").getAsString();
			ruleSet.setSubjectTemplate(new EvaluableExpression(strSubject));
			JsonArray propertiesArray = ruleSetJson.getAsJsonArray("properties");
			for(int index=0; index < propertiesArray.size(); index++) {
				JsonObject jsonProperty = propertiesArray.get(index).getAsJsonObject();
				Rule rule = parseProperty(resourceRuleId, jsonProperty);
				ruleSet.getProperties().add(rule);
			}
		}else {
			throw new MalformedMappingException("provided mapping is missing one or more mandatory keys in the rule set: id, datasources_ids, subject, properties");
		}
		
		
		return ruleSet;
	}
	
	
	private Rule parseProperty(String resourceRuleId, JsonObject jsonProperty) throws MalformedMappingException{
		Rule rule = new Rule();
		if(jsonProperty.has("predicate")) {
			rule.setPredicate(new EvaluableExpression(jsonProperty.get("predicate").getAsString()));
		}else {
			throw new MalformedMappingException("Provided property rule has no mandatory attribute predicate, resource id: "+resourceRuleId);
		}
		if(jsonProperty.has("object")) {
			rule.setObject(new EvaluableExpression(jsonProperty.get("object").getAsString()));
		}else {
			throw new MalformedMappingException("Provided property rule has no mandatory attribute 'expression', resource id: "+resourceRuleId);
		}
		if(jsonProperty.has("is_literal")) {
			rule.setIsLiteral(jsonProperty.get("is_literal").getAsBoolean());
		}else {
			throw new MalformedMappingException("Provided property rule has no mandatory attribute 'is_literal', resource id: "+resourceRuleId);
		}
		if(jsonProperty.has("datatype")){
			rule.setDataType(jsonProperty.get("datatype").getAsString());
		}
		if(jsonProperty.has("lang")){
			rule.setLanguage(jsonProperty.get("lang").getAsString());
		}
		
		return rule;
	}

	// -- Other

	@Override
	public Boolean isCompatible(String mappingContent) {
		Boolean isCompatible = false;
		try {
				JsonObject json = (new Gson()).fromJson(mappingContent, JsonObject.class);
				isCompatible = json.has(DATASOURCES_TOKEN) || json.has(RULES_TOKEN) ; 
		}catch(Exception e) {
			logger.error("provided mapping is not well-expressed in json, check the syntax");
		}
		return isCompatible;
	}

	
	
}
