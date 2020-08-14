package helio.materialiser.data.providers;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.google.gson.JsonObject;
import helio.framework.materialiser.mappings.DataProvider;

public class FileProvider implements DataProvider{

	private static final long serialVersionUID = 1L;
	private File file;
	private static Logger logger = LogManager.getLogger(FileProvider.class);

	public FileProvider() {
		// empty
	}
	
	public FileProvider(File file) {
		this.file = file;
	}
	
	
	public FileProvider(JsonObject arguments) {
		configure(arguments);
	}
	
	@Override
	public InputStream getData() {
		InputStream stream = null;
		try {
			stream = new FileInputStream(file);
		} catch (FileNotFoundException e) {
			logger.error(e.toString());
		}
		return  stream;//new PipedInputStream(new FileReader(file));
	}

	@Override
	public void configure(JsonObject configuration) {
		if(configuration.has("file")) {
			String fileStr = configuration.get("file").getAsString();
			if(fileStr.isEmpty()) {
				throw new IllegalArgumentException("FileProvider needs to receive non empty value for the key 'file'");
			}else{
				this.file = new File(fileStr);
			}
		}else {
			throw new IllegalArgumentException("FileProvider needs to receive json object with the mandatory key 'file'");
		}
		
	}

}
