package helio.materialiser.cache;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
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
import org.eclipse.rdf4j.model.impl.TreeModel;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.query.QueryResults;
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

public class RDF4JMemoryCache implements MaterialiserCache {

	
	
	private static final String LOG_MESSAGE_ERROR_PARSING = "RDF4JMemoryCache:addGraph - error parsing the RDF";
	private static final String LOG_MESSAGE_ERROR_REPOSITORY = "RDF4JMemoryCache:addGraph - repository exception storing the RDF";
	private static final String LOG_MESSAGE_ERROR_PIPE = "RDF4JMemoryCache:solveTupleQuery or  RDF4JMemoryCache:solveGraphQuery- IO exception solving a SPARQL query";
	private static final String ERROR_PARSING_RDF_DURING_DELETING = "RDF4JMemoryCache:deleteGraph or deleteGraphs - error parsing the RDF";
	private static final String ERROR_STORING_RDF_DURING_DELETING = "RDF4JMemoryCache:deleteGraph or deleteGraphs - repository exception storing the rdf";

	
	private static Logger logger = LogManager.getLogger(RDF4JMemoryCache.class);
	
	private Repository repository;
	
	public RDF4JMemoryCache() {
		repository = new SailRepository(new MemoryStore());
		repository.init();
	}

	public RDF4JMemoryCache(File directory) {
		MemoryStore mem = new MemoryStore(directory);
		repository = new SailRepository(mem);
		repository.init();
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
			st = repository.getValueFactory().createStatement(subject, predicate, objectRDF4J, context);
		}else if(triple.getObject().isBlank()){
			st = repository.getValueFactory().createStatement(subject, predicate, createBNode(triple.getObject().toString()), context);
		}else {
			st = repository.getValueFactory().createStatement(subject, predicate, createIRI(triple.getObject().toString()), context);
		}
		
		return st;
	}
	

	private IRI createIRI(String namedGraph) {
		ValueFactory valueFactory = repository.getValueFactory();
		return valueFactory.createIRI(namedGraph);
	}
	
	private BNode createBNode(String bnode) {
		ValueFactory valueFactory = repository.getValueFactory();
		return valueFactory.createBNode(bnode);
	}
	
	private Literal createLiteral(String literal) {
		ValueFactory valueFactory = repository.getValueFactory();
		return valueFactory.createLiteral(literal);
	}
	
	private Literal createLiteralTyped(Literal literal, String datatype) {
		Literal newLiteral = literal;
		if(datatype!=null) {
			ValueFactory valueFactory = repository.getValueFactory();
			newLiteral = valueFactory.createLiteral(literal.stringValue(), createIRI(datatype));
		}
		return newLiteral;
	}
	public Literal createLiteralLang(Literal literal, String lang) {
		Literal newLiteral = literal;
		if(lang!=null) {
			ValueFactory valueFactory = repository.getValueFactory();
			newLiteral = valueFactory.createLiteral(literal.stringValue(), lang);
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
	
	
	/* (non-Javadoc)
	 * @see helio.materialiser.engine.cache.MaterialiserCache#deleteGraph(java.lang.String)
	 */
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
	
	/* (non-Javadoc)
	 * @see helio.materialiser.engine.cache.MaterialiserCache#solveTupleQuery(java.lang.String, helio.framework.objects.SparqlResultsFormat)
	 */
	@Override
	public PipedInputStream solveTupleQuery(String query, SparqlResultsFormat format) {
		PipedInputStream inputStream = null;
		PipedOutputStream outputStream = new PipedOutputStream();
		
		try {
			inputStream = new PipedInputStream(outputStream);
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
		return inputStream;
	}
	
	/* (non-Javadoc)
	 * @see helio.materialiser.engine.cache.MaterialiserCache#solveGraphQuery(java.lang.String, helio.framework.objects.SparqlResultsFormat)
	 */
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
		RepositoryManager repositoryManager = new LocalRepositoryManager(new File("."));
		repositoryManager.addRepositoryConfig(repositoryConfig);
		this.repository = repositoryManager.getRepository(HelioConfiguration.DEFAULT_CACHE_ID);
		
	}

	

}

