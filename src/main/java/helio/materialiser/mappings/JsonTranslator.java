package helio.materialiser.mappings;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.reflections.Reflections;

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
import helio.framework.materialiser.mappings.LinkRule;
import helio.framework.materialiser.mappings.Rule;
import helio.framework.materialiser.mappings.RuleSet;
import helio.materialiser.configuration.HelioConfiguration;
import helio.materialiser.plugins.Plugins;

/**
 * This class implements a {@link MappingTranslator} that translates the Helio Json serialization of the {@link HelioMaterialiserMapping}
 * @author Andrea Cimmino
 *
 */
public class JsonTranslator implements MappingTranslator{

	private static Logger logger = LogManager.getLogger(JsonTranslator.class);
	private static final String DATASOURCES_TOKEN = "datasources";
	private static final String RULES_TOKEN = "resource_rules";
	private static final String DATA_PROVIDER_TOKEN = "provider";
	private static final String DATA_HANDLER_TOKEN = "handler";
	private static final String DATA_SOURCE_ID_TOKEN = "id";
	private static final String DATA_SOURCE_REFRESH_TOKEN = "refresh";
	private static final String LINK_RULES_TOKEN = "link_rules";
	private static final String LINK_RULE_SOURCE_RULESET = "source";
	private static final String LINK_RULE_TARGET_RULESET = "target";
	private static final String LINK_RULE_PREDICATE = "predicate";
	private static final String LINK_RULE_INVERSE_PREDICATE = "inverse";
	private static final String LINK_RULE_CONDITION = "condition";
	
	/**
	 * This constructor initializes the {@link JsonTranslator}
	 */
	public JsonTranslator() {
		//empty
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
			try {
				// add parsed LinkRules
				List<LinkRule> ruleSets = parseLinkRules(json);
				mapping.getLinkRules().addAll(ruleSets);
			}catch(Exception e) {
				logger.error(e.toString());
			}
			
		}catch(Exception e) {
			e.printStackTrace();
			logger.error(e.toString());
		}
		return mapping;
	}
	
	// -- Link Rules methods

	private List<LinkRule> parseLinkRules(JsonObject json) {
		List<LinkRule> linkRules = new ArrayList<>();
		if(json.has(LINK_RULES_TOKEN)) {
			JsonArray dataSourceArray = json.getAsJsonArray(LINK_RULES_TOKEN);
			for(int index=0; index < dataSourceArray.size(); index++) {
				try {
					JsonObject linkRuleJson = dataSourceArray.get(index).getAsJsonObject();
					LinkRule dataSource = initialiseLinkRule( linkRuleJson);
					linkRules.add(dataSource);
				}catch(Exception e) {
					logger.error(e.toString());
				}
			}
		}else {
			logger.warn("Provided mapping has no link rules defined");
		}
		return linkRules;
	}

	
	private LinkRule initialiseLinkRule(JsonObject linkRuleJson) throws MalformedMappingException {
		LinkRule rule = new LinkRule();
		if(linkRuleJson.has(LINK_RULE_CONDITION)) {
			rule.setExpression(new EvaluableExpression(linkRuleJson.get(LINK_RULE_CONDITION).getAsString()));
		}else {
			throw new MalformedMappingException("provided mapping is missing mandatory key in linking rule: "+LINK_RULE_CONDITION);
		}
		if(linkRuleJson.has(LINK_RULE_SOURCE_RULESET)) {
			rule.setSourceNamedGraph(linkRuleJson.get(LINK_RULE_SOURCE_RULESET).getAsString());
		}else {
			throw new MalformedMappingException("provided mapping is missing mandatory key in linking rule: "+LINK_RULE_SOURCE_RULESET);
		}
		if(linkRuleJson.has(LINK_RULE_TARGET_RULESET)) {
			rule.setTargetNamedGraph(linkRuleJson.get(LINK_RULE_TARGET_RULESET).getAsString());		
		}else {
			throw new MalformedMappingException("provided mapping is missing mandatory key in linking rule: "+LINK_RULE_TARGET_RULESET);
		}
		if(linkRuleJson.has(LINK_RULE_PREDICATE)) {
			rule.setPredicate(linkRuleJson.get(LINK_RULE_PREDICATE).getAsString());
		}else {
			logger.warn("Provided mapping lacks of a property to link the source and target subjects");
		}
		if(linkRuleJson.has(LINK_RULE_INVERSE_PREDICATE)) {
			rule.setInversePredicate(linkRuleJson.get(LINK_RULE_INVERSE_PREDICATE).getAsString());
		}else {
			logger.warn("Provided mapping lacks of an inverse property to link the source and target subjects");
		}
		if(!linkRuleJson.has(LINK_RULE_PREDICATE) && !linkRuleJson.has(LINK_RULE_INVERSE_PREDICATE))
			throw new MalformedMappingException("provided mapping is missing one of the mandatory keys either "+LINK_RULE_PREDICATE+" or "+LINK_RULE_INVERSE_PREDICATE);
		
		return rule;
	}


	// -- Data Source methods
	

	private List<DataSource> parseDataSources(JsonObject json) throws MalformedMappingException{
		List<DataSource> dss = new ArrayList<>();
		if(json.has(DATASOURCES_TOKEN)) {
			JsonArray dataSourceArray = json.getAsJsonArray(DATASOURCES_TOKEN);
			for(int index=0; index < dataSourceArray.size(); index++) {
				try {
				JsonObject dataSourceJson = dataSourceArray.get(index).getAsJsonObject();
				DataSource dataSource = initialiseDataSource(dataSourceJson);
				dss.add(dataSource);
				}catch(Exception e) {
					logger.error(e.toString());
				}
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
		Reflections reflections = new Reflections(HelioConfiguration.DEFAULT_DATA_INTERACTORS_PACKAGE);    
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
				dataHandler = dataHandlerClass.getConstructor().newInstance();
			}else { 
				// 3.1 try to find the provider in the plugins
				dataHandler = Plugins.buildDataHandlerByName(dataHandlerClassName);
			}
			if(dataHandler ==null) {
				throw new MalformedMappingException(" specified data handler does not exists: " + dataHandlerClassName);
			}else {
				dataHandler.configure(jsonObject);
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
		Reflections reflections = new Reflections(HelioConfiguration.DEFAULT_DATA_INTERACTORS_PACKAGE);
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
				dataProvider = Plugins.buildDataProviderByName(dataProviderClassName);
		
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
	
	

	// -- Rule sets methods
	
	private List<RuleSet> parseRuleSets(JsonObject json) throws MalformedMappingException{
		List<RuleSet> rss = new ArrayList<>();
		if(json.has(RULES_TOKEN)) {
			JsonArray ruleSetArray = json.getAsJsonArray(RULES_TOKEN);
			for(int index=0; index < ruleSetArray.size(); index++) {
				try {
					JsonObject ruleSetJson = ruleSetArray.get(index).getAsJsonObject();
					RuleSet ruleSet = initialiseRuleSet(ruleSetJson);
					rss.add(ruleSet);
				}catch(Exception e) {
					logger.error(e.toString());
				}
			}
		}else {
			logger.warn("Provided mapping has no data resource rules defined");
		}
		return rss;
	}
	

	private RuleSet initialiseRuleSet(JsonObject ruleSetJson) throws MalformedMappingException {
		RuleSet ruleSet = new RuleSet();
		if(ruleSetJson.has("id") && ruleSetJson.has("datasource_ids")) {
			
			String resourceRuleId = ruleSetJson.get("id").getAsString();
			ruleSet.setResourceRuleId(resourceRuleId);
			ruleSetJson.get("datasource_ids").getAsJsonArray().forEach(ds-> ruleSet.getDatasourcesId().add(ds.getAsString()));
			if(ruleSetJson.has("subject")) {
				String strSubject = ruleSetJson.get("subject").getAsString();
				ruleSet.setSubjectTemplate(new EvaluableExpression(strSubject));
			}else {
				logger.warn("provided mapping is missing key in the rule set: subject");
			}
			if(ruleSetJson.has("properties")) {
				JsonArray propertiesArray = ruleSetJson.getAsJsonArray("properties");
				for(int index=0; index < propertiesArray.size(); index++) {
					JsonObject jsonProperty = propertiesArray.get(index).getAsJsonObject();
					Rule rule = parseProperty(resourceRuleId, jsonProperty);
					ruleSet.getProperties().add(rule);
				}
			}else {
				logger.warn("provided mapping is missing key in the rule set: properties");
			}
		}else {
			throw new MalformedMappingException("provided mapping is missing one or more mandatory keys in the rule set: id, datasource_ids");
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
			logger.warn("provided mapping is not compatible with JsonTranslator");
		}
		return isCompatible;
	}

	
	
}
