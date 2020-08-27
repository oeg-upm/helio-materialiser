package helio.materialiser.data.handlers;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.google.gson.JsonObject;

import helio.framework.materialiser.mappings.DataHandler;

public class RegexHandler implements DataHandler{

	private static final long serialVersionUID = 1L;
	private String iterator;
	private static final String ITERATOR_KEY = "iterator";
	private static Logger logger = LogManager.getLogger(RegexHandler.class);
	
	public RegexHandler() {
		super();
	}
	
	@Override
	public Queue<String> splitData(InputStream dataStream) {
		ConcurrentLinkedQueue<String> queueOfresults = new ConcurrentLinkedQueue<>();
		if(dataStream!=null) {
			String textDocument = readTextDocument(dataStream);
			if(textDocument!=null && !textDocument.isEmpty()) {
				Pattern pattern = Pattern.compile(iterator);
		        Matcher matcher = pattern.matcher(textDocument);
		        // check all occurance
		        while (matcher.find()) {
		        		queueOfresults.add(matcher.group());
		        }
			}else {
				logger.warn("Given expression does not match anything in the retrieved text document");
			}
		}
		return queueOfresults;
	}
	
	public String readTextDocument(InputStream dataStream) {
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
		List<String> fitleredValues = new ArrayList<>();
		Pattern pattern = Pattern.compile(filter);
        Matcher matcher = pattern.matcher(dataChunk);
        // check all occurance
        while (matcher.find()) {
            fitleredValues.add(matcher.group());
        }
		return fitleredValues;
	}

	@Override
	public void configure(JsonObject configuration) {
		if(configuration.has(ITERATOR_KEY)) {
			iterator = configuration.get(ITERATOR_KEY).getAsString();
			if(iterator.isEmpty())
				throw new IllegalArgumentException("RegexHandler needs to receive non empty value for the key 'iterator'");
		}else {
			throw new IllegalArgumentException("RegexHandler needs to receive json object with the mandatory key 'iterator'");
		}
	}

}
