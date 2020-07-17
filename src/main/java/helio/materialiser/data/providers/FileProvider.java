package helio.materialiser.data.providers;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

import com.google.gson.JsonObject;

import helio.framework.materialiser.mappings.DataProvider;

public class FileProvider implements DataProvider{

	private static final long serialVersionUID = 1L;
	private File file;
	
	public FileProvider(JsonObject arguments) {
		if(arguments.has("file")) {
			String fileStr = arguments.get("file").getAsString();
			if(fileStr.isEmpty()) {
				throw new IllegalArgumentException("JsonHandler needs to receive non empty value for the key 'file'");
			}else{
				this.file = new File(fileStr);
			}
		}else {
			throw new IllegalArgumentException("JsonHandler needs to receive json object with the mandatory key 'file'");
		}
	}
	
	@Override
	public InputStream getData() {
		InputStream stream = null;
		try {
			stream = new FileInputStream(file);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		return  stream;//new PipedInputStream(new FileReader(file));
	}

}
