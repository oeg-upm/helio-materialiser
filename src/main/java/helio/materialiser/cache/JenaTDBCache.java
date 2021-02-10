package helio.materialiser.cache;

import java.io.ByteArrayOutputStream;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.tdb.TDBFactory;

import helio.framework.materialiser.MaterialiserCache;
import helio.framework.objects.SparqlResultsFormat;
import helio.materialiser.configuration.HelioConfiguration;

public class JenaTDBCache implements MaterialiserCache{

	private String tdbName;
	
	public JenaTDBCache() {
		tdbName = HelioConfiguration.DEFAULT_H2_PERSISTENT_CACHE_DIRECTORY+"/jena-tdb";
	}
	
	@Override
	public void configureRepository(String arg0) {
		// TODO Auto-generated method stub
		// TDBFactory.setup(Location location, StoreParams params) -> https://jena.apache.org/documentation/tdb/store-parameters.html
	}
	
	@Override
	public void addGraph(String arg0, Model arg1) {
		 Dataset dataset = TDBFactory.createDataset(tdbName);
		 dataset.begin(ReadWrite.WRITE) ;
		 dataset.addNamedModel(arg0, arg1);
		 dataset.commit();
		 dataset.end();
	}



	@Override
	public void deleteGraph(String arg0) {
		 Dataset dataset = TDBFactory.createDataset(tdbName);
		 dataset.begin(ReadWrite.WRITE) ;
		 dataset.removeNamedModel(arg0);
		 dataset.commit();
		 dataset.end();
	}

	@Override
	public void deleteGraphs() {
		 Dataset dataset = TDBFactory.createDataset(tdbName);
		 dataset.begin(ReadWrite.WRITE) ;
		 dataset.listNames().forEachRemaining(uri -> dataset.removeNamedModel(uri));
		 dataset.commit();
		 dataset.end();
		
	}

	@Override
	public Model getGraph(String arg0) {
		 Model model = ModelFactory.createDefaultModel(); 
		 Dataset dataset = TDBFactory.createDataset(tdbName);
		 dataset.begin(ReadWrite.READ) ;
		 model= dataset.getNamedModel(arg0);
		 dataset.commit();
		 dataset.end();
		 return model;
	}

	@Override
	public Model getGraphs() {
		 Dataset dataset = TDBFactory.createDataset(tdbName);
		 dataset.begin(ReadWrite.READ) ;
		 Model model = dataset.getUnionModel();
		 dataset.commit();
		 dataset.end();
		 return model;
	}

	@Override
	public Model getGraphs(String... arg0) {
		 Model model = ModelFactory.createDefaultModel(); 
		 Dataset dataset = TDBFactory.createDataset(tdbName);
		 dataset.begin(ReadWrite.READ) ;
		 for(int index=0; index < arg0.length; index++) {
			 model.add(dataset.getNamedModel(arg0[index]));
		 }
		 dataset.commit();
		 dataset.end();
		 return model;
	}

	@Override
	public Model solveGraphQuery(String arg0) {
		Dataset dataset = TDBFactory.createDataset(tdbName);
		dataset.begin(ReadWrite.READ);
		Query query = QueryFactory.create(arg0);
		QueryExecution qexec = QueryExecutionFactory.create(query, dataset);
		Model output = ModelFactory.createDefaultModel();
		if(query.isConstructType() || query.isDescribeType()) {
			output = qexec.execDescribe();
		}else {
			// throw exception
		}
		dataset.end();
		return output;
	}

	@Override
	public String solveTupleQuery(String arg0, SparqlResultsFormat format) {
		Dataset dataset = TDBFactory.createDataset(tdbName);
		dataset.begin(ReadWrite.READ);
		Query query = QueryFactory.create(arg0);
		QueryExecution qexec = QueryExecutionFactory.create(query, dataset);
		StringBuilder output = new StringBuilder();
		if (query.isAskType()) {
			output.append(solveAsk(qexec, format));
		} else if (query.isSelectType()) {
			output.append(solveSelect(qexec, format));
		} else {
			// throw exception
		}

		dataset.end();
		return null;
	}

	private String solveAsk(QueryExecution qexec, SparqlResultsFormat format) {
		StringBuilder output = new StringBuilder();
		boolean result = qexec.execAsk();
		if(SparqlResultsFormat.XML.equals(format)) {
			output.append(ResultSetFormatter.asXMLString(result));
		}else if(SparqlResultsFormat.CSV.equals(format)) {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ResultSetFormatter.outputAsCSV(baos, result);
			output.append(new String(baos.toByteArray()));
		}else if(SparqlResultsFormat.TSV.equals(format)) {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ResultSetFormatter.outputAsTSV(baos, result);
			output.append(new String(baos.toByteArray()));
		}else if(SparqlResultsFormat.JSON.equals(format)) {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ResultSetFormatter.outputAsJSON(baos, result);
			output.append(new String(baos.toByteArray()));
		}else {
			//throw an exception
		}
		
		return output.toString();
	}
	
	private String solveSelect(QueryExecution qexec, SparqlResultsFormat format) {
		StringBuilder output = new StringBuilder();
		ResultSet result = qexec.execSelect();
		if(SparqlResultsFormat.XML.equals(format)) {
			output.append(ResultSetFormatter.asXMLString(result));
		}else if(SparqlResultsFormat.CSV.equals(format)) {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ResultSetFormatter.outputAsCSV(baos, result);
			output.append(new String(baos.toByteArray()));
		}else if(SparqlResultsFormat.TSV.equals(format)) {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ResultSetFormatter.outputAsTSV(baos, result);
			output.append(new String(baos.toByteArray()));
		}else if(SparqlResultsFormat.JSON.equals(format)) {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ResultSetFormatter.outputAsJSON(baos, result);
			output.append(new String(baos.toByteArray()));
		}else {
			//throw an exception
		}
		
		return output.toString();
	}
}
