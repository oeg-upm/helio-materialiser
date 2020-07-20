package helio.materialiser.data.providers;

import java.io.PipedInputStream;

import helio.framework.materialiser.mappings.DataProvider;

public class InMemoryProvider implements DataProvider{

	private static final long serialVersionUID = 1L;
	public PipedInputStream pipedData;
	
	public InMemoryProvider(PipedInputStream pipedData) {
		this.pipedData = pipedData;
	}
	
	@Override
	public PipedInputStream getData() {
		return pipedData;
	}

}
