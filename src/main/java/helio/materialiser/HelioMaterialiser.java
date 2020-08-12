package helio.materialiser;

import java.io.File;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.parser.ParsedGraphQuery;
import org.eclipse.rdf4j.query.parser.ParsedOperation;
import org.eclipse.rdf4j.query.parser.ParsedTupleQuery;
import org.eclipse.rdf4j.query.parser.QueryParserUtil;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;

import helio.framework.exceptions.ResourceNotFoundException;
import helio.framework.materialiser.Evaluator;
import helio.framework.materialiser.MaterialiserCache;
import helio.framework.materialiser.MaterialiserEngine;
import helio.framework.materialiser.mappings.HelioMaterialiserMapping;
import helio.framework.objects.SparqlResultsFormat;
import helio.materialiser.cache.RDF4JMemoryCache;
import helio.materialiser.evaluator.H2Evaluator;


// TODO: check if a hash of the retrieved source can enhance the speed
// TODO: make the repository been used configurable, and maybe other parameters
// TODO: implement linking
public class HelioMaterialiser implements MaterialiserEngine {

	
	
	private MaterialiserOrchestrator orchestrator;
	private static Logger logger = LogManager.getLogger(HelioMaterialiser.class);

	public static final MaterialiserCache HELIO_CACHE = new RDF4JMemoryCache(new File("helio-cache"));
	public static final Evaluator EVALUATOR = new H2Evaluator();

	
	public HelioMaterialiser(HelioMaterialiserMapping mappings) {
		orchestrator = new MaterialiserOrchestrator(mappings);
		
	}
	
	@Override
	public void close() {
		orchestrator.close();
	}
	
	
	@Override
	public void updateSynchronousSources() {
		orchestrator.updateSynchronousSources();
	}
	
	
	@Override
	public PipedInputStream getStreamResource(String iri, RDFFormat format) throws ResourceNotFoundException {
		PipedInputStream inputStream = null;
		Model model = HELIO_CACHE.getGraph(iri);
		if(model!=null && model.isEmpty()) {
			throw new ResourceNotFoundException(iri);
		}else {
			inputStream = fromModelToStream(model, format);
		}
		return inputStream;
	}

	@Override
	public PipedInputStream getStreamRDF(RDFFormat format) {
		Model model =  HELIO_CACHE.getGraphs();
		return fromModelToStream(model, format);
	}

	@Override
	public PipedInputStream queryStream(String sparqlQuery, SparqlResultsFormat format) {
		// TODO: TRY AND CATCH!
		PipedInputStream pipedInputStream = null;
		ParsedOperation operation = QueryParserUtil.parseOperation(QueryLanguage.SPARQL, sparqlQuery, null); 
		
		if (operation instanceof ParsedTupleQuery) {
			pipedInputStream =  HELIO_CACHE.solveTupleQuery(sparqlQuery, format);
		} else if (operation instanceof ParsedGraphQuery) {
			pipedInputStream =  HELIO_CACHE.solveGraphQuery(sparqlQuery, format);
		} else {
			// TODO: throw exception 
		}
	
		return pipedInputStream;
	}
	
	
	
	private PipedInputStream fromModelToStream(Model model, RDFFormat format) {
		PipedInputStream inputStream = null;
		PipedOutputStream outputStream = new PipedOutputStream();
		try {
			inputStream = new PipedInputStream(outputStream);
			Rio.write(model, outputStream, format);
		} catch (IOException e) {
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
	
	public String transformToString(PipedInputStream  input) {
		StringBuilder translatedStream = new StringBuilder();
		try {
			 int data = input.read();
			 while(data != -1){
					translatedStream.append((char) data);
		            data = input.read();
		        }
				
		} catch (IOException e) {
			logger.error(e.toString());
		} finally {
				try {
					input.close();
				} catch (IOException e) {
					logger.error(e.toString());
				}
			
		}
		return translatedStream.toString();
	}

	@Override
	public String getResource(String iri, RDFFormat format) throws ResourceNotFoundException {
		return transformToString(getStreamResource(iri, format));
	}

	@Override
	public String getRDF(RDFFormat format) {
		return transformToString(getStreamRDF(format));
	}

	@Override
	public String query(String sparqlQuery, SparqlResultsFormat format) {
		return transformToString(queryStream(sparqlQuery, format));
	}





	
}
