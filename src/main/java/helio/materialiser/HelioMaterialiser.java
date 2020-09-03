package helio.materialiser;

import java.io.StringWriter;
import java.io.Writer;

import org.apache.jena.rdf.model.Model;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.parser.ParsedBooleanQuery;
import org.eclipse.rdf4j.query.parser.ParsedGraphQuery;
import org.eclipse.rdf4j.query.parser.ParsedOperation;
import org.eclipse.rdf4j.query.parser.ParsedTupleQuery;
import org.eclipse.rdf4j.query.parser.QueryParserUtil;

import helio.framework.exceptions.ResourceNotFoundException;
import helio.framework.materialiser.MaterialiserEngine;
import helio.framework.materialiser.mappings.HelioMaterialiserMapping;
import helio.framework.objects.SparqlResultsFormat;
import helio.materialiser.configuration.HelioConfiguration;
import org.apache.jena.shared.impl.JenaParameters;

/**
 * This class is an implementation of the interface {@link MaterialiserEngine}
 * @author Andrea Cimmino
 *
 */
public class HelioMaterialiser implements MaterialiserEngine {

	private MaterialiserOrchestrator orchestrator;
	private static Logger logger = LogManager.getLogger(HelioMaterialiser.class);
	
	
	/**
	 * This constructor receives a valid {@link HelioMaterialiserMapping} object, if provided mapping is empty no RDF will be generated
	 * @param mappings a valid {@link HelioMaterialiserMapping} object
	 */
	public HelioMaterialiser(HelioMaterialiserMapping mappings) {
		JenaParameters.enableSilentAcceptanceOfUnknownDatatypes=true;
		JenaParameters.enableEagerLiteralValidation = true;
		JenaParameters.disableBNodeUIDGeneration=true;
		orchestrator = new MaterialiserOrchestrator(mappings);
		HelioConfiguration.EVALUATOR.initH2Cache();
	}
		
	@Override
	public void close() {
		orchestrator.close();
		HelioConfiguration.EVALUATOR.closeH2Cache();
	}
	
	
	@Override
	public void updateSynchronousSources() {
		orchestrator.updateSynchronousSources();
	}
	
		
	@Override
	public Model getResource(String iri) throws ResourceNotFoundException {
		String query = HelioUtils.concatenate("CONSTRUCT { <",iri,"> ?p ?o  } WHERE { <",iri,"> ?p ?o . }");
		Model model = HelioConfiguration.HELIO_CACHE.solveGraphQuery(query);
		if(model!=null && model.isEmpty()) {
			throw new ResourceNotFoundException(iri);
		}
		return model;
	}

	@Override
	public Model getRDF() {
		return  HelioConfiguration.HELIO_CACHE.getGraphs();
	}

	@Override
	public String query(String sparqlQuery, SparqlResultsFormat format) {
		String queryResults = null;
		ParsedOperation operation = QueryParserUtil.parseOperation(QueryLanguage.SPARQL, sparqlQuery, null); 
		
		if (operation instanceof ParsedTupleQuery) {
			queryResults =  HelioConfiguration.HELIO_CACHE.solveTupleQuery(sparqlQuery, format);
		} else if (operation instanceof ParsedGraphQuery) {
			Model model =  HelioConfiguration.HELIO_CACHE.solveGraphQuery(sparqlQuery);
			Writer writer = new StringWriter();
			try {
				model.write(writer,format.getFormat(), HelioConfiguration.DEFAULT_BASE_URI);
				queryResults = writer.toString();
			}finally {
				try {
					writer.close();
				}catch(Exception e) {
					logger.error(e.toString());
				}
			}
			
		} else if(operation instanceof ParsedBooleanQuery){
			queryResults =  HelioConfiguration.HELIO_CACHE.solveTupleQuery(sparqlQuery, format);
		}else {
			logger.warn("Query is not valid or is unsupported, currently supported queries: Select, Ask, Construct, and Describe");
		}
		return queryResults;
	}





	
}
