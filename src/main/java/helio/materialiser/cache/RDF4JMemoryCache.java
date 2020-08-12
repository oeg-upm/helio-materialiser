package helio.materialiser.cache;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.query.QueryResults;
import org.eclipse.rdf4j.query.resultio.sparqljson.SPARQLResultsJSONWriter;
import org.eclipse.rdf4j.query.resultio.sparqlxml.SPARQLResultsXMLWriter;
import org.eclipse.rdf4j.query.resultio.text.csv.SPARQLResultsCSVWriter;
import org.eclipse.rdf4j.query.resultio.text.tsv.SPARQLResultsTSVWriter;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.util.Repositories;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.sail.memory.MemoryStore;

import helio.framework.materialiser.MaterialiserCache;
import helio.framework.objects.SparqlResultsFormat;
import helio.materialiser.configuration.HelioConfiguration;

public class RDF4JMemoryCache implements MaterialiserCache {

	
	
	private static final String LOG_MESSAGE_ERROR_PARSING = "RDF4JMemoryCache:addGraph - error parsing the RDF";
	private static final String LOG_MESSAGE_ERROR_REPOSITORY = "RDF4JMemoryCache:addGraph - repository exception storing the RDF";
	private static final String LOG_MESSAGE_ERROR_IO = "RDF4JMemoryCache:addGraph - IO exception storing the rdf";
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
	public void changeRepository(Repository repository) {
		this.repository = repository;
		repository.init();
	}
	
	
	
	/* (non-Javadoc)
	 * @see helio.materialiser.engine.cache.MaterialiserCache#addGraph(java.lang.String, java.lang.String, org.eclipse.rdf4j.rio.RDFFormat)
	 */
	@Override
	public void addGraph(String namedGraph, String rdf, RDFFormat format) {
		IRI context = createIRI(namedGraph);
		Repositories.consume(repository, conn -> {
			InputStream inputDataStream = null;
			try {
				inputDataStream = new ByteArrayInputStream(rdf.getBytes());
				conn.add(inputDataStream, HelioConfiguration.DEFAULT_BASE_URI, format, context);
			} catch (RDFParseException e) {
				logger.error(LOG_MESSAGE_ERROR_PARSING);
				logger.error(e.toString());
			} catch (RepositoryException e) {
				logger.error(LOG_MESSAGE_ERROR_REPOSITORY);
				logger.error(e.toString());
			} catch (IOException e) {
				logger.error(LOG_MESSAGE_ERROR_IO);
				logger.error(e.toString());
			} finally {
				if (inputDataStream != null) {
					try {
						inputDataStream.close();
					} catch (IOException e) {
						logger.error(e.toString());
					}
				}
			}

		});

	}
	
	/* (non-Javadoc)
	 * @see helio.materialiser.engine.cache.MaterialiserCache#addGraph(java.lang.String, java.io.InputStream, org.eclipse.rdf4j.rio.RDFFormat)
	 */
	@Override
	public void addGraph(String namedGraph, InputStream inputDataStream, RDFFormat format) {
		IRI context = createIRI(namedGraph);
		Repositories.consume(repository, conn -> {
			try {
				conn.add(inputDataStream, HelioConfiguration.DEFAULT_BASE_URI, format, context);
			} catch (RDFParseException e) {
				logger.error(LOG_MESSAGE_ERROR_PARSING);
				logger.error(e.toString());
			} catch (RepositoryException e) {
				logger.error(LOG_MESSAGE_ERROR_REPOSITORY);
				logger.error(e.toString());
			} catch (IOException e) {
				logger.error(LOG_MESSAGE_ERROR_IO);
				logger.error(e.toString());
			} finally {
				if (inputDataStream != null) {
					try {
						inputDataStream.close();
					} catch (IOException e) {
						logger.error(e.toString());
					}
				}
			}

		});

	}
	
	/*  TO REMOVE ----> */
	public IRI createIRI(String namedGraph) {
		ValueFactory valueFactory = repository.getValueFactory();
		return valueFactory.createIRI(namedGraph);
	}
	
	public Literal createLiteral(String literal) {
		ValueFactory valueFactory = repository.getValueFactory();
		return valueFactory.createLiteral(literal);
	}
	
	public Literal createLiteralTyped(Literal literal, String datatype) {
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
	
	/*  ---> TO REMOVE */
	
	/* (non-Javadoc)
	 * @see helio.materialiser.engine.cache.MaterialiserCache#getGraph(java.lang.String)
	 */
	@Override
	public Model getGraph(String namedGraph){
		IRI context = createIRI(namedGraph);		
		Model model = new LinkedHashModel();
		Repositories.consumeNoTransaction(repository, conn -> {
			RepositoryResult<Statement> fragments = conn.getStatements(null, null, null, context);
			model.addAll(QueryResults.asModel(fragments));	
		});
	
		return model;
	}
	
	/* (non-Javadoc)
	 * @see helio.materialiser.engine.cache.MaterialiserCache#getGraphs()
	 */
	@Override
	public Model getGraphs() {
		
		// Retrieve data
		Model model = new LinkedHashModel();
		Repositories.consume(repository, conn -> {
			IRI[] iris = new IRI[] {};
			RepositoryResult<Statement> fragments = conn.getStatements(null, null, null, iris);
			model.addAll(QueryResults.asModel(fragments));
		});
		//String query = "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }";
		//return Repositories.graphQueryNoTransaction(repository, query, r -> QueryResults.asModel(r));
		return model;
	}
	
	
	
	
	/* (non-Javadoc)
	 * @see helio.materialiser.engine.cache.MaterialiserCache#getGraphs(java.lang.String)
	 */
	@Override
	public Model getGraphs(String ... namedGraphs){
		// Adapt contexts
		IRI[] contexts = new IRI[namedGraphs.length];
		for(int index=0; index <namedGraphs.length; index++) {
			contexts[index] = createIRI(namedGraphs[index]);
		}
		// Retrieve data
		Model model = new LinkedHashModel();
		Repositories.consumeNoTransaction(repository, conn -> {
			RepositoryResult<Statement> fragments = conn.getStatements(null, null, null, contexts);
			model.addAll(QueryResults.asModel(fragments));	
		});
	
		return model;
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
	public PipedInputStream solveGraphQuery(String query, SparqlResultsFormat format) {
		PipedInputStream inputStream = null;
		PipedOutputStream outputStream = new PipedOutputStream();
		try {
			inputStream = new PipedInputStream(outputStream);
			Model model = Repositories.graphQueryNoTransaction(repository, query, r -> QueryResults.asModel(r));
			if(format.equals(SparqlResultsFormat.RDF_XML)) {
				Rio.write(model, outputStream, RDFFormat.RDFXML);
			}else if (format.equals(SparqlResultsFormat.JSON_LD)) {
				Rio.write(model, outputStream, RDFFormat.JSONLD);
			}else if (format.equals(SparqlResultsFormat.N_TRIPLES) || format.equals(SparqlResultsFormat.N_TRIPLE)) {
				Rio.write(model, outputStream, RDFFormat.NTRIPLES);
			}else if (format.equals(SparqlResultsFormat.RDF_TURTLE)) {
				Rio.write(model, outputStream, RDFFormat.TURTLE);
			}else if (format.equals(SparqlResultsFormat.RDF_N3)) {
				Rio.write(model, outputStream, RDFFormat.N3);
			}else{
				// throw exception
			}
			
		} catch (IOException e) {
			logger.error(LOG_MESSAGE_ERROR_PIPE);
			logger.error(e.toString());
		} finally {
			try {
				outputStream.close();
			} catch (IOException e) {
				logger.error(e.toString());
			}
		}
		
		
		return inputStream;
	}


}

