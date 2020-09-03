package helio.materialiser.cache;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.Optional;
import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.datatypes.TypeMapper;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.AnonId;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.impl.TreeModel;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.QueryResults;
import org.eclipse.rdf4j.query.parser.ParsedBooleanQuery;
import org.eclipse.rdf4j.query.parser.ParsedOperation;
import org.eclipse.rdf4j.query.parser.ParsedTupleQuery;
import org.eclipse.rdf4j.query.parser.QueryParserUtil;
import org.eclipse.rdf4j.query.resultio.sparqljson.SPARQLResultsJSONWriter;
import org.eclipse.rdf4j.query.resultio.sparqlxml.SPARQLResultsXMLWriter;
import org.eclipse.rdf4j.query.resultio.text.csv.SPARQLResultsCSVWriter;
import org.eclipse.rdf4j.query.resultio.text.tsv.SPARQLResultsTSVWriter;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.config.RepositoryConfig;
import org.eclipse.rdf4j.repository.config.RepositoryConfigSchema;
import org.eclipse.rdf4j.repository.manager.LocalRepositoryManager;
import org.eclipse.rdf4j.repository.manager.RepositoryManager;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.util.Repositories;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.sail.memory.MemoryStore;

import helio.framework.materialiser.MaterialiserCache;
import helio.framework.objects.SparqlResultsFormat;
import helio.materialiser.HelioUtils;
import helio.materialiser.configuration.HelioConfiguration;

/**
 * This class implements a {@link MaterialiserCache} relying on the RDF4J repositories.<p> 
 * Using the configuration method of this class the inner repository of the {@link RDF4JMemoryCache} can be instantiated using different implementations.<p>
 * @author Andrea Cimmino
 *
 */
public class RDF4JMemoryCache implements MaterialiserCache {

	private static final String LOG_MESSAGE_ERROR_PARSING = "RDF4JMemoryCache:addGraph - error parsing the RDF";
	private static final String LOG_MESSAGE_ERROR_REPOSITORY = "RDF4JMemoryCache:addGraph - repository exception storing the RDF";
	private static final String LOG_MESSAGE_ERROR_PIPE = "RDF4JMemoryCache:solveTupleQuery or  RDF4JMemoryCache:solveGraphQuery- IO exception solving a SPARQL query";
	private static final String ERROR_PARSING_RDF_DURING_DELETING = "RDF4JMemoryCache:deleteGraph or deleteGraphs - error parsing the RDF";
	private static final String ERROR_STORING_RDF_DURING_DELETING = "RDF4JMemoryCache:deleteGraph or deleteGraphs - repository exception storing the rdf";
	private static Logger logger = LogManager.getLogger(RDF4JMemoryCache.class);
	private static final ValueFactory VALUE_FACTORY = SimpleValueFactory.getInstance();
	private Repository repository;
	
	private static final String REPOSITORY_CONFIGURATION = "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>.\n" + 
			"@prefix rep: <http://www.openrdf.org/config/repository#>.\n" + 
			"@prefix sr: <http://www.openrdf.org/config/repository/sail#>.\n" + 
			"@prefix sail: <http://www.openrdf.org/config/sail#>.\n" + 
			"@prefix ms: <http://www.openrdf.org/config/sail/memory#>.\n" + 
			"\n" + 
			"[] a rep:Repository ;\n" + 
			"   rep:repositoryID \"helio-storage\" ; # this id must be the same id in the Helio configuration file $.repository.id\n" + 
			"   rep:repositoryImpl [\n" + 
			"      rep:repositoryType \"openrdf:SailRepository\" ;\n" + 
			"      sr:sailImpl [\n" + 
			"         sail:sailType \"openrdf:MemoryStore\" ;\n" + 
			"         ms:persist true ; # false if repository is in memory\n" + 
			"         ms:syncDelay 0\n" + 
			"      ]\n" + 
			"   ].";
	/**
	 * This method initializes the inner repository with a {@link MemoryStore} repository
	 */
	public RDF4JMemoryCache() {
		repository = new SailRepository(new MemoryStore());
		repository.init();
		configureRepository(REPOSITORY_CONFIGURATION);
	}
	
	
	
	@Override
	public void addGraph(String namedGraph, Model model) {
		IRI context = createIRI(namedGraph);
		Repositories.consume(repository, conn -> {
			try {
				model.listStatements().forEachRemaining(st -> conn.add(toRDF4JStatement(context, st.asTriple()), context));
			} catch (RDFParseException e) {
				logger.error(LOG_MESSAGE_ERROR_PARSING);
				logger.error(e.toString());
			} catch (RepositoryException e) {
				logger.error(LOG_MESSAGE_ERROR_REPOSITORY);
				logger.error(e.toString());
			}
		});

	}
	
	/**
	 * This method translates a Jena {@link Triple} into a RDF4J {@link Statement} allocated in the provided named graph
	 * @param context the named graph where the statement should be allocated
	 * @param triple a Jena {@link Triple}
	 * @return an equivalent and valid RDF4J {@link Statement}
	 */
	private Statement toRDF4JStatement(IRI context, Triple triple) {
		Resource subject = null;
		if(triple.getSubject().isBlank()) {
			subject = createBNode(triple.getSubject().toString());
		}else {
			subject = createIRI(triple.getSubject().toString());
		}
		 
		IRI predicate = createIRI(triple.getPredicate().toString());
		Statement st =  null;
		if(triple.getObject().isLiteral()) {
			String jenaLiteral = triple.getObject().getLiteral().getLexicalForm();
			String datatype = triple.getObject().getLiteralDatatype().getURI();
			String lang = triple.getObject().getLiteralLanguage();
			Literal objectRDF4J = createLiteral(jenaLiteral);
			if(lang!=null && !lang.isEmpty()) {
				objectRDF4J = createLiteralLang(objectRDF4J, lang);
			}else if(datatype!=null && !datatype.isEmpty() ) {
				objectRDF4J = createLiteralTyped(objectRDF4J, datatype);
			}
			st = VALUE_FACTORY.createStatement(subject, predicate, objectRDF4J, context);
		}else if(triple.getObject().isBlank()){
			st = VALUE_FACTORY.createStatement(subject, predicate, createBNode(triple.getObject().toString()), context);
		}else {
			st = VALUE_FACTORY.createStatement(subject, predicate, createIRI(triple.getObject().toString()), context);
		}
		
		return st;
	}
	
	/**
	 * This method creates a valid {@link IRI} 
	 * @param namedGraph a {@link String} IRI
	 * @return a valid {@link IRI} 
	 */
	private IRI createIRI(String namedGraph) {
		return VALUE_FACTORY.createIRI(namedGraph);
	}
	

	/**
	 * This method creates a valid {@link BNode} 
	 * @param bnode a {@link String} blank node
	 * @return a valid {@link BNode} 
	 */
	private BNode createBNode(String bnode) {
		return VALUE_FACTORY.createBNode(bnode);
	}
	
	/**
	 * This method creates a valid {@link Literal} 
	 * @param literal a {@link String} literal
	 * @return a valid {@link Literal} 
	 */
	private Literal createLiteral(String literal) {
		return VALUE_FACTORY.createLiteral(literal);
	}
	
	/**
	 * This method adds to an existing {@link Literal} a provided data type
	 * @param literal a {@link Literal}
	 * @param datatype the URL of a specific data type
	 * @return a valid {@link Literal} that has the provided data type
	 */
	private Literal createLiteralTyped(Literal literal, String datatype) {
		Literal newLiteral = literal;
		if(datatype!=null) {
			newLiteral = VALUE_FACTORY.createLiteral(literal.stringValue(), createIRI(datatype));
		}
		return newLiteral;
	}
	
	/**
	 * This method adds to an existing {@link Literal} a provided language tag
	 * @param literal a {@link Literal}
	 * @param lang a language tag, e.g., 'en' or 'es'
	 * @return a valid {@link Literal} that has the provided language tag
	 */
	public Literal createLiteralLang(Literal literal, String lang) {
		Literal newLiteral = literal;
		if(lang!=null) {
			newLiteral = VALUE_FACTORY.createLiteral(literal.stringValue(), lang);
		}
		return newLiteral;
	}
	
	
	
	@Override
	public Model getGraphs() {
		String query = "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o . }";                                               
		return solveGraphQuery( query); 
	}
	
	@Override
	public Model getGraph(String namedGraph){
		String query = HelioUtils.concatenate("CONSTRUCT {  ?s ?p ?o  } WHERE { GRAPH <",namedGraph,"> { ?s ?p ?o . } }");    
		return solveGraphQuery( query);

	}

	@Override
	public Model getGraphs(String ... namedGraphs){		
		StringBuilder graphs = new StringBuilder();
		for(int index=0; index < namedGraphs.length; index++) {
			graphs.append("<").append(namedGraphs[index]).append("> ");      
		}
		String query = HelioUtils.concatenate("CONSTRUCT {  ?s ?p ?o } WHERE { GRAPH ?graph { ?s ?p ?o .} VALUES ?graph { ",graphs.toString(),"} } " );                         
		return solveGraphQuery( query); 

	}
	
	@Override
	public void deleteGraph(String namedGraph) {
		IRI context = createIRI(namedGraph);
		Repositories.consume(repository, conn -> {
			try {
				conn.clear(context);
			} catch (RDFParseException e) {
				logger.error(ERROR_PARSING_RDF_DURING_DELETING);
				logger.error(e.toString());
			} catch (RepositoryException e) {
				logger.error(ERROR_STORING_RDF_DURING_DELETING);
				logger.error(e.toString());
			}
		});

	}

	@Override
	public void deleteGraphs() {
		Repositories.consume(repository, conn -> {
			try {
				IRI[] iris = new IRI[] {};
				conn.clear(iris);
			} catch (RDFParseException e) {
				logger.error(ERROR_PARSING_RDF_DURING_DELETING);
				logger.error(e.toString());
			} catch (RepositoryException e) {
				logger.error(ERROR_STORING_RDF_DURING_DELETING);
				logger.error(e.toString());
			}
		});
	}
	
	@Override
	public String solveTupleQuery(String query, SparqlResultsFormat format) {
		String queryResult = null;
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		ParsedOperation operation = QueryParserUtil.parseOperation(QueryLanguage.SPARQL, query, null); 
		try {
			if(operation instanceof ParsedBooleanQuery){
				Boolean partialQueryResult = repository.getConnection().prepareBooleanQuery(query).evaluate();
				queryResult = formatASKResult(partialQueryResult, format);
			}else if (operation instanceof ParsedTupleQuery) {
				if(format.equals(SparqlResultsFormat.CSV)) {
					Repositories.tupleQuery(repository, query, new SPARQLResultsCSVWriter(outputStream));
				}else if (format.equals(SparqlResultsFormat.JSON)) {
					Repositories.tupleQuery(repository, query, new SPARQLResultsJSONWriter(outputStream));
				}else if (format.equals(SparqlResultsFormat.XML)) {
					Repositories.tupleQuery(repository, query, new SPARQLResultsXMLWriter(outputStream));
				}else if (format.equals(SparqlResultsFormat.TSV)) {
					Repositories.tupleQuery(repository, query, new SPARQLResultsTSVWriter(outputStream));
				} else {
					// throw exception
				}
				queryResult = outputStream.toString( "UTF-8" );
			}			
		} catch (IOException e) {
			logger.error(LOG_MESSAGE_ERROR_PIPE);
			logger.error(e.toString());
		}finally {
			try {
				outputStream.close();
			} catch (IOException e) {
				logger.error(e.toString());
			}
		}
		
		return queryResult;
	}
	
	
	
	
	private String formatASKResult(Boolean partialQueryResult, SparqlResultsFormat format) {
		StringBuilder builder = new StringBuilder();
		if(format.equals(SparqlResultsFormat.JSON)) {
			builder.append("{ \"head\" : { \"link\": [] } ,  \"boolean\" : ").append(partialQueryResult).append("}");
		}else if(format.equals(SparqlResultsFormat.CSV) || format.equals(SparqlResultsFormat.TSV)) {
			builder.append("\"bool\"\n");
			String result = "0";
			if(partialQueryResult)
				result = "1";
			builder.append(result);
		}else if(format.equals(SparqlResultsFormat.XML)) {
			builder.append("<?xml version=\"1.0\"?>\n<sparql xmlns=\"http://www.w3.org/2005/sparql-results#\">\n\t<head></head>\n\t<boolean>").append(partialQueryResult).append("</boolean>\n</sparql>");
		}
		
		return builder.toString();
	}



	@Override
	public Model solveGraphQuery(String query) {
		Model modelJena = ModelFactory.createDefaultModel();
		try {
			org.eclipse.rdf4j.model.Model modelRDF4J = Repositories.graphQueryNoTransaction(repository, query, r ->  QueryResults.asModel(r));
			modelRDF4J.stream().forEach( st -> transformStatement(st, modelJena));
		} catch (Exception e) {
			logger.error(LOG_MESSAGE_ERROR_PIPE);
			logger.error(e.toString());
		}
		return modelJena;
	}
	
	
	/**
	 * This method adds a RDF4J {@link Statement} into a Jena {@link Model}
	 * @param st a RDF4J {@link Statement}
	 * @param model a Jena {@link Model} where the RDF4J {@link Statement} will be added
	 */
	private void transformStatement(Statement st, Model model) {
		Resource subject = st.getSubject();
		Resource predicate = st.getPredicate();
		Value object = st.getObject();
		org.apache.jena.rdf.model.Resource subjectJena = ResourceFactory.createResource(subject.stringValue());
		if(subject instanceof BNode)
			subjectJena = model.createResource(AnonId.create(subject.stringValue()));
		if(object instanceof Literal) {
			Literal objectLiteral = (Literal) object;
			RDFNode node = ResourceFactory.createPlainLiteral(objectLiteral.stringValue());
			Optional<String> language = objectLiteral.getLanguage();
			if(language.isPresent()) {
				node = ResourceFactory.createLangLiteral(objectLiteral.stringValue(), language.get());
			}else if(objectLiteral.getDatatype()!=null) {
				TypeMapper mapper = new TypeMapper();
				String dataTypeString = objectLiteral.getDatatype().stringValue();
				RDFDatatype rdfDataTypeJena = mapper.getSafeTypeByName(dataTypeString);
				node = ResourceFactory.createTypedLiteral(objectLiteral.stringValue(), rdfDataTypeJena);
				
			}
			model.add(subjectJena, ResourceFactory.createProperty(predicate.stringValue()), node);
			
		}else if(object instanceof IRI) {
			IRI objectIRI = (IRI) object;
			model.add(subjectJena,ResourceFactory.createProperty(predicate.stringValue()), ResourceFactory.createResource(objectIRI.stringValue()));
		}else if(object instanceof BNode) {
			String blankObject = ((BNode) object).getID();
			org.apache.jena.rdf.model.Resource createdObject = model.createResource(AnonId.create(blankObject));
			
			model.add(subjectJena, ResourceFactory.createProperty(predicate.stringValue()),createdObject);
			
			
		}
	}
	
	
	@Override
	public void configureRepository(String configuration) {
		InputStream inputStream =  null;
		// Read the file into a model
		Model model = ModelFactory.createDefaultModel();
		try {
			inputStream = new ByteArrayInputStream(configuration.getBytes(Charset.forName("UTF-8")));
			model = ModelFactory.createDefaultModel();
			model.read(inputStream, HelioConfiguration.DEFAULT_BASE_URI, "TURTLE");
		
		}catch(Exception e) {
				try {
					inputStream.close();
				} catch (IOException e1) {
					logger.error(e.toString());
				}
		}
		// translate to rdf4j model
		TreeModel graph = new TreeModel();
		StmtIterator stmtIterator = model.listStatements();
		while(stmtIterator.hasNext()) {
			graph.add(toRDF4JStatement(null,stmtIterator.next().asTriple()));
		}
		// Setup the repo
		Iterator<Statement> uniqueResourceIt = graph.getStatements(null, RDF.TYPE, RepositoryConfigSchema.REPOSITORY).iterator();
		RepositoryConfig repositoryConfig = null;
		int index=0;
		while(uniqueResourceIt.hasNext()) {
			if(index>0)
				throw new IllegalArgumentException("RDF4JMemoryCache expects a unique subjet with the type rep:Repository");
			repositoryConfig = RepositoryConfig.create(graph, uniqueResourceIt.next().getSubject());
			index++;
		}
		RepositoryManager repositoryManager = new LocalRepositoryManager(new File(HelioConfiguration.DEFAULT_H2_PERSISTENT_CACHE_DIRECTORY));
		repositoryManager.addRepositoryConfig(repositoryConfig);
		this.repository = repositoryManager.getRepository(HelioConfiguration.DEFAULT_CACHE_ID);
		//this.repository.init();
	}

	

}

