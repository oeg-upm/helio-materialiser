package helio.materialiser.data.handlers;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import com.google.gson.JsonObject;
import helio.framework.materialiser.mappings.DataHandler;

public class HtmlHandler implements DataHandler {

	private static final long serialVersionUID = 1L;
	private String iterator;
	private static final String ITERATOR_KEY = "iterator";
	private static Logger logger = LogManager.getLogger(HtmlHandler.class);

	public HtmlHandler() {
		super();
	}
	
	@Override
	public Queue<String> splitData(InputStream dataStream) {
		ConcurrentLinkedQueue<String> queueOfresults = new ConcurrentLinkedQueue<>();
		if(dataStream!=null) {
			String htmlDocument = readHTMLDocument(dataStream);
			if(htmlDocument!=null && !htmlDocument.isEmpty()) {
				// 2. Parse raw data into HTML Document
				Document doc = Jsoup.parse(htmlDocument);
				// 3. Apply expression
				Elements elements = doc.select(this.iterator);
				// 4. Retrieve chunks of data
				Iterator<Element> it = elements.iterator();
				while(it.hasNext()) {
					Element element = it.next();
					queueOfresults.add(element.toString());
				}
			}else {
				logger.warn("Given expression does not match anything in the retrieved html document");
			}
		}
		return queueOfresults;
	}
	
	public String readHTMLDocument(InputStream dataStream) {
		StringBuilder builder = new StringBuilder();
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(dataStream));
			String line;
			while ((line = br.readLine()) != null) {
				builder.append(line);
			}
			br.close();
			dataStream.close();
		} catch (Exception e) {
			logger.error(e.toString());
		}
		return builder.toString();
	}

	@Override
	public List<String> filter(String filter, String dataChunk) {
		List<String> results = null;
		try {
			// 1. Parse raw data into HTML Document
			Document doc = Jsoup.parse(dataChunk);
			// 2. Apply expression
			Elements elements = doc.select(filter);
			// 3. Retrieve data values
			Iterator<Element> iterator = elements.iterator();
			results = new ArrayList<>();
			int index = 0;
			while(iterator.hasNext()) {
				Element element = iterator.next();
				results.add(element.html());
				index++;
			}
			if(index == 0) {
				results = null;
				logger.error("Literal not found with the data reference provided: "+filter);
			}
		}catch(Exception e) {
			logger.error("Literal (2) not found with the data reference provided: "+filter);
		}
		
		return results;
	}

	@Override
	public void configure(JsonObject configuration) {
		if(configuration.has(ITERATOR_KEY)) {
			this.iterator = configuration.get(ITERATOR_KEY).getAsString();
			if(this.iterator.isEmpty())
				throw new IllegalArgumentException("HtmlHandler needs to receive non empty value for the key 'iterator'");
		}else {
			throw new IllegalArgumentException("HtmlHandler needs to receive json object with the mandatory key 'iterator'");
		}
		
	}

}
