package helio.materialiser.data.handlers;

import java.io.InputStream;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import com.google.gson.JsonObject;

import helio.framework.materialiser.mappings.DataHandler;
import helio.materialiser.configuration.HelioConfiguration;

public class RDFHandler implements DataHandler {

	private static final String MIME_KEY = "mime";
	private static final long serialVersionUID = 1L;
	private static Logger logger = LogManager.getLogger(RDFHandler.class);
	private String mime;
	
	
	@Override
	public Queue<String> splitData(InputStream dataStream) {
		ConcurrentLinkedQueue<String> queueOfresults = new ConcurrentLinkedQueue<>();
		Model model = null;
		try {
			IRI[] iris = new IRI[] {};
			Optional<RDFFormat> parsedFormatOptional = Rio.getParserFormatForMIMEType(mime);
			if(parsedFormatOptional.isPresent()) {
				model = Rio.parse(dataStream, HelioConfiguration.DEFAULT_BASE_URI, parsedFormatOptional.get(), iris);
				// split model by subjects
				List<Resource> subjects = model.subjects().stream().filter(subject -> !(subject instanceof BNode)).collect(Collectors.toList());
				for(int index=0; index < subjects.size(); index++) {
					Resource subject = subjects.get(index);
					Resource[] contexts  = new Resource[] {};
					
					Model subModel = (new ModelBuilder()).build();
					subModel.addAll(model.filter(subject, null, null, contexts));
					
					recursiveNavigation( subModel,  model);
					Writer writer = new StringWriter();
					Rio.write(subModel, writer, HelioConfiguration.DEFAULT_BASE_URI, HelioConfiguration.DEFAULT_RDF_FORMAT);
					queueOfresults.add(writer.toString());
				}
			}else {
				logger.error("Provided mime type is not supported, please provide a valid one");
			}
			
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("An error occured parsing the RDF");
		} 

		return queueOfresults;
	}
	
	
	
	private void recursiveNavigation(Model augmentableModel, Model originalModel) {
		ValueFactory valueFactory = SimpleValueFactory.getInstance();
		List<BNode> blankNodes = originalModel.objects().stream().filter(obj -> obj instanceof BNode).map(obj -> valueFactory.createBNode(obj.stringValue())).collect(Collectors.toList());
		for(int index=0; index < blankNodes.size(); index++) {
			BNode blankNode = blankNodes.get(index);
			navigation(blankNode, augmentableModel, originalModel);
		}
	}
	
	private void navigation(BNode blankNode, Model augmentableModel, Model originalModel) {
		ValueFactory valueFactory = SimpleValueFactory.getInstance();
		Resource[] contexts = new Resource[] {};
		Iterator<Statement> stIterator = originalModel.getStatements(blankNode, null, null, contexts).iterator();
		List<Statement> statemets = new ArrayList<>();
		while (stIterator.hasNext()) {
			Statement st = stIterator.next();
			statemets.add(st);
			if (st.getObject() instanceof BNode) {
				// recursive call
				navigation(valueFactory.createBNode(st.getObject().toString()), augmentableModel, originalModel);
			}
		}
		augmentableModel.addAll(statemets);
	}

	
	

	@Override
	public List<String> filter(String filter, String dataChunk) {
		// TODO Auto-generated method stub
		return null;
	}

	
	@Override
	public void configure(JsonObject configuration) {
		if(configuration.has(MIME_KEY)) {
			this.mime = configuration.get(MIME_KEY).getAsString();
			if(this.mime.isEmpty())
				throw new IllegalArgumentException("RDFHandler needs to receive non empty value for the key 'mime'");
		}else {
			throw new IllegalArgumentException("RDFHandler needs to receive json object with the mandatory key 'mime'");
		}
		
	}
}