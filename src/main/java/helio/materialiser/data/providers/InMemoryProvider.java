package helio.materialiser.data.providers;

import java.io.PipedInputStream;

import com.google.gson.JsonObject;

import helio.framework.materialiser.mappings.DataProvider;

public class InMemoryProvider implements DataProvider{

	private static final long serialVersionUID = 1L;
	public PipedInputStream pipedData;
	
	
	public InMemoryProvider() {
		//empty
	}
	
	public InMemoryProvider(PipedInputStream pipedData) {
		this.pipedData = pipedData;
	}
	
	@Override
	public PipedInputStream getData() {
		return pipedData;
	}

	@Override
	public void configure(JsonObject configuration) {
		// empty
	}

}
