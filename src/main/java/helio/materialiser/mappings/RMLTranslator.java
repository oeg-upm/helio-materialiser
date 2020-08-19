package helio.materialiser.mappings;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.javatuples.Quartet;

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
import helio.materialiser.data.handlers.CsvHandler;
import helio.materialiser.data.handlers.JsonHandler;
import helio.materialiser.data.handlers.XmlHandler;
import helio.materialiser.data.providers.FileProvider;
import helio.materialiser.data.providers.URLProvider;

public class RMLTranslator implements MappingTranslator {


	
	private static ValueFactory factory = SimpleValueFactory.getInstance();

	private static final IRI RML_LOGICAL_SOURCE_PROPERTY = factory.createIRI("http://semweb.mmlab.be/ns/rml#logicalSource");
    private static final IRI RML_SOURCE_PROPERTY = factory.createIRI("http://semweb.mmlab.be/ns/rml#source");
    private static final IRI RML_REFERENCE_FORMULATION_PROPERTY = factory.createIRI("http://semweb.mmlab.be/ns/rml#referenceFormulation");
    private static final IRI RML_ITERATOR_PROPERTY = factory.createIRI("http://semweb.mmlab.be/ns/rml#iterator");
    
    private static final IRI RML_SUBJECT_PROPERTY = factory.createIRI("http://www.w3.org/ns/r2rml#subject");
    private static final IRI RML_SUBJECTMAP_PROPERTY = factory.createIRI("http://www.w3.org/ns/r2rml#subjectMap");
    private static final IRI RML_SUBJECT_CLASS_PROPERTY = factory.createIRI("http://www.w3.org/ns/r2rml#class");
    
    
    private static final IRI RML_PREDICATE_OBJECT_MAP_PROPERTY = factory.createIRI("http://www.w3.org/ns/r2rml#predicateObjectMap");
    private static final IRI RML_PREDICATE_PROPERTY = factory.createIRI("http://www.w3.org/ns/r2rml#predicate");
    private static final IRI RML_PREDICATE_MAP_PROPERTY = factory.createIRI("http://www.w3.org/ns/r2rml#predicateMap");
    private static final IRI RML_OBJECT_PROPERTY = factory.createIRI("http://www.w3.org/ns/r2rml#OBJECT");
    private static final IRI RML_OBJECT_MAP_PROPERTY = factory.createIRI("http://www.w3.org/ns/r2rml#objectMap");


    private static final IRI RML_IRI_TEMPLATE_PROPERTY = factory.createIRI("http://www.w3.org/ns/r2rml#template");
   	private static final IRI RML_IRI_CONSTANT_PROPERTY = factory.createIRI("http://www.w3.org/ns/r2rml#constant");
    private static final IRI RML_IRI_REFERENCE_PROPERTY = factory.createIRI("http://semweb.mmlab.be/ns/rml#reference");
    
    private static final IRI RML_PARENT_TRIPLEMAP_PROPERTY = factory.createIRI("http://www.w3.org/ns/r2rml#parentTriplesMap");
    


    private static final IRI RML_DATATYPE  = factory.createIRI("http://www.w3.org/ns/r2rml#datatype");
    private static final IRI RML_LANG  = factory.createIRI("http://www.w3.org/ns/r2rml#language");
    
  
    private static final IRI RML_QL_JSONPATH = factory.createIRI("http://semweb.mmlab.be/ns/ql#JSONPath");
    private static final IRI RML_QL_CSV = factory.createIRI("http://semweb.mmlab.be/ns/ql#CSV");
    private static final IRI RML_QL_XPATH = factory.createIRI("http://semweb.mmlab.be/ns/ql#XPath");
	private static final IRI[] CONTEXTS = new IRI[] {};

	private static final String TOKEN_PROPERTIES = " properties";
	private static Logger logger = LogManager.getLogger(RMLTranslator.class);

	public RMLTranslator() {
		//empty
	}
	
	@Override
	public Boolean isCompatible(String arg0) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public HelioMaterialiserMapping translate(String content) throws MalformedMappingException {
		HelioMaterialiserMapping mapping = new HelioMaterialiserMapping();
		InputStream inputStream = new ByteArrayInputStream(content.getBytes());
		try {
			Model model = Rio.parse(inputStream, HelioConfiguration.DEFAULT_BASE_URI, RDFFormat.TURTLE);
			mapping = parseMapping(model);
		} catch (Exception e) {
			logger.error(e.toString());
		} finally {
			try {
				inputStream.close();
			} catch (IOException e) {
				logger.error(e.toString());
			}
		}
		return mapping;
	}

	private HelioMaterialiserMapping parseMapping(Model model) {
		HelioMaterialiserMapping mapping = new HelioMaterialiserMapping();
		
		Iterable<Statement> statements = model.getStatements(null, RML_LOGICAL_SOURCE_PROPERTY, null, CONTEXTS);
		statements.forEach(st -> {
			try {
				String dataSourceId = st.getObject().stringValue();
				DataSource dataSource = parseDataSource( st.getObject(), model);
				mapping.getDatasources().add(dataSource);
				String format = dataSource.getDataHandler().getClass().getSimpleName().replace("Handler", "").toLowerCase();
				RuleSet ruleSet =  parseRuleSer( st.getSubject(), model, dataSourceId, format);
				mapping.getRuleSets().add(ruleSet);
				
				// TODO : Link Rules?
			} catch (MalformedMappingException e) {
				logger.error(e.toString());
			}
		});
		
		return mapping;
	}

	
	
	// Parsing the RuleSet
	

	private RuleSet parseRuleSer(Value documentRootIRI, Model model, String dataSourceId, String format) throws MalformedMappingException {
		// subject template
		String evaluableMappingSubject = extractEvaluableMappingSubject(documentRootIRI, model, format);

		RuleSet ruleSet = new RuleSet();
		ruleSet.getDatasourcesId().add(dataSourceId);
		ruleSet.setResourceRuleId(concatStrings(evaluableMappingSubject,"#",String.valueOf(dataSourceId.hashCode())));
		ruleSet.setSubjectTemplate(new EvaluableExpression(evaluableMappingSubject));
		
		ruleSet.getProperties().addAll(extractRules(documentRootIRI, model, format));
		
		return ruleSet;
	}

	private List<Rule> extractRules(Value documentRootIRI, Model model, String format){
		List<Rule> rules = new ArrayList<>();
		// the rr:class
		 Rule typeRule = extractRdfTypeRule(documentRootIRI, model);
		 if(typeRule!=null)
			 rules.add(typeRule);
		// the other properties
		List<Value> propertyObjectMapsSubjects = getRangeValues(documentRootIRI, RML_PREDICATE_OBJECT_MAP_PROPERTY, model);
		for(int index=0; index < propertyObjectMapsSubjects.size(); index++) {
			Value propertyObjectMapsSubject = propertyObjectMapsSubjects.get(index);
			try {
				Rule newRule = extractObjectMapRule(propertyObjectMapsSubject, model, format);
				rules.add(newRule);
			}catch(Exception e) {
				logger.error(e.toString());
			}
		}
		
		return rules;
	}

	private Rule extractObjectMapRule(Value objectMapSubject, Model model, String format) throws MalformedMappingException {
		Rule newRule = new Rule();
		String predicate = extractPredicate(objectMapSubject, model, format);
		Quartet<String,String, String, Boolean> object = extractObject(objectMapSubject, model, format);
		
		newRule.setPredicate(new EvaluableExpression(predicate));
		newRule.setObject(new EvaluableExpression(object.getValue0()));
		newRule.setDataType(object.getValue1());
		newRule.setLanguage(object.getValue2());
		newRule.setIsLiteral(object.getValue3());
		
		return newRule;
	}
	
	private Quartet<String,String, String, Boolean> extractObject(Value objectPredicateMapSubject, Model model, String format) throws MalformedMappingException {
		Quartet<String,String, String, Boolean> quarted = Quartet.with(null, null, null, null);
		String objectTemplate =  getUnitaryRange(objectPredicateMapSubject, RML_OBJECT_PROPERTY, model);
		if(objectTemplate==null) {
			Value objectMapSubject = getUnitaryRangeValue(objectPredicateMapSubject, RML_OBJECT_MAP_PROPERTY, model);
			if(objectMapSubject==null) {
				throw new MalformedMappingException(concatStrings("Missing object to generate the RDF, specify it using either ",RML_OBJECT_PROPERTY.toString()," or ",RML_OBJECT_MAP_PROPERTY.toString(), TOKEN_PROPERTIES));
			}else {
				objectTemplate = getUnitaryRange(objectMapSubject, RML_IRI_REFERENCE_PROPERTY, model);
				if(objectTemplate==null) {
					// entails this can be an object property
					objectTemplate = getUnitaryRange(objectMapSubject, RML_IRI_TEMPLATE_PROPERTY, model);
					if(objectTemplate==null)
						objectTemplate = getUnitaryRange(objectMapSubject, RML_IRI_CONSTANT_PROPERTY, model);
					if(objectTemplate!=null) {
						quarted = Quartet.with(objectTemplate, null, null, false);
					}else {
						throw new MalformedMappingException(concatStrings("Missing object specification, specify it using either ",RML_IRI_CONSTANT_PROPERTY.toString(),", ",RML_IRI_REFERENCE_PROPERTY.toString()," or ", RML_IRI_TEMPLATE_PROPERTY.toString()));
					}
				}else {
					String datatype = getUnitaryRange(objectMapSubject, RML_DATATYPE, model);
					String lang = getUnitaryRange(objectMapSubject, RML_LANG, model);
					if(format.equals("json")) {
						objectTemplate = concatStrings("{$.",objectTemplate,"}");
					}else if(format.equals("xml")) {
						objectTemplate = concatStrings("{//",objectTemplate,"}");
					}
					quarted = Quartet.with(objectTemplate, datatype, lang, true);
				}
			}
		}else {
			quarted.setAt0(objectTemplate);
			quarted.setAt3(false);
		}
		return quarted;
	}

	private String extractPredicate(Value objectMapSubject, Model model, String format) throws MalformedMappingException {
		String predicateTemplate =  getUnitaryRange(objectMapSubject, RML_PREDICATE_PROPERTY, model);
		if(predicateTemplate==null) {
			Value predicateMapSubject = getUnitaryRangeValue(objectMapSubject, RML_PREDICATE_MAP_PROPERTY, model);
			if(predicateMapSubject==null) {
				throw new MalformedMappingException(concatStrings("Missing predicate to generate the RDF, specify it using either ",RML_PREDICATE_PROPERTY.toString()," or ",RML_PREDICATE_MAP_PROPERTY.toString(), TOKEN_PROPERTIES));
			}else {
				predicateTemplate = getUnitaryRange(objectMapSubject, RML_IRI_CONSTANT_PROPERTY, model);
				if(predicateTemplate==null)
					predicateTemplate = getUnitaryRange(objectMapSubject, RML_IRI_TEMPLATE_PROPERTY, model);
				if(predicateTemplate==null) {
					throw new MalformedMappingException(concatStrings("Missing predicate specification, specify it using either ",RML_IRI_CONSTANT_PROPERTY.toString()," or ", RML_IRI_TEMPLATE_PROPERTY.toString()));
				}else {
					if(format.equals("json")) {
						predicateTemplate = predicateTemplate.replaceAll("\\{", "{$.");
					}else if(format.equals("xml")) {
						predicateTemplate = predicateTemplate.replaceAll("\\{", "{");
					}
				}
			}
		}
		return predicateTemplate;
	}

	private Rule extractRdfTypeRule(Value documentRootIRI, Model model) {
		Rule classRule = null;
		Value subjectMapSubject = getUnitaryRangeValue(documentRootIRI, RML_SUBJECTMAP_PROPERTY, model);
		if(subjectMapSubject!=null) {
			String rdfTypeIRI = getUnitaryRange(subjectMapSubject, RML_SUBJECT_CLASS_PROPERTY, model);
			if(rdfTypeIRI!=null) {
				classRule = new Rule();
				classRule.setIsLiteral(false);
				classRule.setObject(new EvaluableExpression(rdfTypeIRI));
			}
		}
		return classRule;
	}


	
	private String extractEvaluableMappingSubject(Value documentRootIRI, Model model, String format) throws MalformedMappingException {
		String subjectExpression =  getUnitaryRange(documentRootIRI, RML_SUBJECT_PROPERTY, model);
		if(subjectExpression==null) {
			Value subjectMapSubject = getUnitaryRangeValue(documentRootIRI, RML_SUBJECTMAP_PROPERTY, model);
			if(subjectMapSubject==null) {
				throw new MalformedMappingException(concatStrings("Missing subject to generate the RDF, specify it using either ",RML_SUBJECT_PROPERTY.toString()," or ",RML_SUBJECTMAP_PROPERTY.toString(), TOKEN_PROPERTIES));
			}else {
				subjectExpression = getUnitaryRange(subjectMapSubject, RML_IRI_CONSTANT_PROPERTY, model);
				if(subjectExpression==null)
					subjectExpression = getUnitaryRange(subjectMapSubject, RML_IRI_TEMPLATE_PROPERTY, model);
				if(subjectExpression==null) {
					throw new MalformedMappingException(concatStrings("Missing subject for triples that will be generated, specify it using either ",RML_IRI_TEMPLATE_PROPERTY.toString()," or "+RML_IRI_CONSTANT_PROPERTY.toString(), TOKEN_PROPERTIES));
				}else {
					if(format.equals("json")) {
						subjectExpression = subjectExpression.replaceAll("\\{", "\\{$.");
					}else if(format.equals("xml")) {
						subjectExpression = subjectExpression.replaceAll("\\{", "\\{//");
					}
				}
			}
		}
		
		
		return subjectExpression;
	}
	
	
	
	// Parsing the DataSource
	
	private DataSource parseDataSource(Value subject, Model model) throws MalformedMappingException {
	
		String source = getUnitaryRange(subject, RML_SOURCE_PROPERTY, model);
		if(source==null) {
			throw new MalformedMappingException("Provided mapping lacks of mandatory property "+RML_SOURCE_PROPERTY);
		}
		Boolean isFile =!source.startsWith("http") && !source.startsWith("ftp");
			
		String iterator = getUnitaryRange(subject, RML_ITERATOR_PROPERTY, model);
		String referenceFormulation = getUnitaryRange(subject, RML_REFERENCE_FORMULATION_PROPERTY, model);
		if(referenceFormulation==null)
			throw new MalformedMappingException("Provided mapping lacks of mandatory property "+RML_REFERENCE_FORMULATION_PROPERTY);
		
		return buildDataSource(subject.stringValue(), source, referenceFormulation.toLowerCase(), iterator, isFile);
		
	}
	
	private DataSource buildDataSource(String id, String source, String referenceFormulation, String iterator, Boolean isFile) throws MalformedMappingException {
		// 
		DataProvider provider = null;
		if(isFile) {
			provider = new FileProvider(new File(source));
		}else {
			provider = new URLProvider(source);
		}
		// 
		DataHandler handler = null;
		if(RML_QL_CSV.toString().toLowerCase().contains(referenceFormulation.toLowerCase())) {
			handler = new CsvHandler(",");
			logger.warn("CSV file must have the name of the columns as first row, columns must be separated by ',' and contain no text delimitator");
		}else if(RML_QL_JSONPATH.toString().toLowerCase().contains(referenceFormulation.toLowerCase())) {
			if(iterator ==null || iterator.isEmpty()){
				throw new MalformedMappingException("JSON format requires an iterator specified with "+RML_ITERATOR_PROPERTY);
			}else {
				handler = new JsonHandler(iterator);
			}
		}else if(RML_QL_XPATH.toString().toLowerCase().contains(referenceFormulation.toLowerCase())) {
			if(iterator ==null || iterator.isEmpty()){
				throw new MalformedMappingException("Xml format requires an iterator specified with "+RML_ITERATOR_PROPERTY);
			}else {
				handler = new XmlHandler(iterator);
			}
		}else {
			throw new MalformedMappingException("Current implementation only supports CSV, XML, or JSON");
		}
		//
		return  new DataSource(id, handler, provider);
	}
	
	
	
	
	
	// Ancillary methods
	
	private String getUnitaryRange(Value subject, IRI property, Model model) {
		String output = null;
		Resource subjectIRI = null;
		if(subject instanceof BNode) {
			subjectIRI = factory.createBNode(subject.stringValue());
		}else if( subject instanceof IRI) {
			subjectIRI = factory.createIRI(subject.stringValue());
		}else {
			// throw something ?
		}
		
		Iterator<Statement> iterator = model.getStatements(subjectIRI, property, null, CONTEXTS).iterator();
		while(iterator.hasNext()) {
			output = iterator.next().getObject().stringValue();
			break;
		}
		
		return output;
	}
	
	private Value getUnitaryRangeValue(Value subject, IRI property, Model model) {
		Value output = null;
		Resource subjectIRI = null;
		if(subject instanceof BNode) {
			subjectIRI = factory.createBNode(subject.stringValue());
		}else if( subject instanceof IRI) {
			subjectIRI = factory.createIRI(subject.stringValue());
		}else {
			// throw something ?
		}
		
		Iterator<Statement> iterator = model.getStatements(subjectIRI, property, null, CONTEXTS).iterator();
		while(iterator.hasNext()) {
			output = iterator.next().getObject();
			break;
		}
		
		return output;
	}
	
	private List<Value> getRangeValues(Value subject, IRI property, Model model) {
		List<Value> output = new ArrayList<>();
		Resource subjectIRI = null;
		if(subject instanceof BNode) {
			subjectIRI = factory.createBNode(subject.stringValue());
		}else if( subject instanceof IRI) {
			subjectIRI = factory.createIRI(subject.stringValue());
		}else {
			// throw something ?
		}
		
		Iterator<Statement> iterator = model.getStatements(subjectIRI, property, null, CONTEXTS).iterator();
		while(iterator.hasNext()) {
			output.add(iterator.next().getObject());
		}
		
		return output;
	}
	
	private String concatStrings(String str1, String str2, String str3) {
		StringBuilder builder = new StringBuilder();
		builder.append(str1).append(str2).append(str3);
		return builder.toString();
	}

	private String concatStrings(String str1, String str2, String str3, String str4) {
		StringBuilder builder = new StringBuilder();
		builder.append(str1).append(str2).append(str3).append(str4);
		return builder.toString();
	}
	
	private String concatStrings(String str1, String str2, String str3, String str4, String str5) {
		StringBuilder builder = new StringBuilder();
		builder.append(str1).append(str2).append(str3).append(str4).append(str5);
		return builder.toString();
	}
	
	private String concatStrings(String str1, String str2, String str3, String str4, String str5, String str6) {
		StringBuilder builder = new StringBuilder();
		builder.append(str1).append(str2).append(str3).append(str4).append(str5).append(str6);
		return builder.toString();
	}
}
