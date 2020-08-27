package helio.materialiser.data.handlers;

import java.io.InputStream;
import java.io.StringWriter;
import java.io.Writer;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.google.gson.JsonObject;
import helio.framework.materialiser.mappings.DataHandler;
import helio.materialiser.configuration.HelioConfiguration;

public class RDFHandler implements DataHandler {

	private static final String MIME_KEY = "format";
	private static final long serialVersionUID = 1L;
	private static Logger logger = LogManager.getLogger(RDFHandler.class);
	private String format;
	
	// TODO: ALLOW DEFINING A SPECIFIC BASE URI
	
	public RDFHandler() {
		super();
	}
	
	@Override
	public Queue<String> splitData(InputStream dataStream) {
		ConcurrentLinkedQueue<String> queueOfresults = new ConcurrentLinkedQueue<>();
		
		try {
			Model model = ModelFactory.createDefaultModel();
			model.read(dataStream, HelioConfiguration.DEFAULT_BASE_URI, format);	
			Writer stringWriter = new StringWriter();
			model.write(stringWriter,  HelioConfiguration.DEFAULT_RDF_FORMAT, HelioConfiguration.DEFAULT_BASE_URI);
			queueOfresults.add(stringWriter.toString());
			stringWriter.close();
		} catch (Exception e) {
			logger.error(e.toString());
			logger.error("An error occured parsing the RDF");
		} 

		return queueOfresults;
	}
	
	
	

	
	

	@Override
	public List<String> filter(String filter, String dataChunk) {
		// TODO Auto-generated method stub
		return null;
	}

	
	@Override
	public void configure(JsonObject configuration) {
		if(configuration.has(MIME_KEY)) {
			this.format = configuration.get(MIME_KEY).getAsString();
			if(this.format.isEmpty())
				throw new IllegalArgumentException("RDFHandler needs to receive non empty value for the key 'format'");
		}else {
			throw new IllegalArgumentException("RDFHandler needs to receive json object with the mandatory key 'format'");
		}
		
	}
}