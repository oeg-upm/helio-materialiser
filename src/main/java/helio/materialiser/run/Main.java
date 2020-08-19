package helio.materialiser.run;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.eclipse.rdf4j.sail.elasticsearchstore.ElasticsearchStore;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.http.HTTPRepository;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sparql.SPARQLRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import helio.framework.materialiser.MappingTranslator;
import helio.framework.materialiser.mappings.HelioMaterialiserMapping;
import helio.materialiser.HelioMaterialiser;
import helio.materialiser.configuration.HelioConfiguration;
import helio.materialiser.mappings.AutomaticTranslator;
import virtuoso.rdf4j.driver.VirtuosoRepository;

public class Main {

	private static final String HELP_ARGUMENT = "--h";
	private static final String MAPPINGS_ARGUMENT = "--mappings=";
	private static final String CACHE_ARGUMENT = "--cache=";
	private static final String PLUGINS_ARGUMENT = "--plugins=";
	private static final String CLOSE_ARGUMENT = "--close";
	private static final String WRITE_ARGUMENT = "--write=";

	private static final String MAN = "\n" + 
			"[Main command] java -jar helio.jar\n" + 
			"\n" + 
			"Usage:\n" + 
			"	--mappings= (mandatory): specifies the directory where the mappings file are located (bear in mind that Helio will put together all the mappings files creating a single one in memory)\n" + 
			"	--cache (optional): specifies the type of cache that Helio will use. Check https://github.com/oeg-upm/Helio/wiki/Helio-Generator-&-Mappings#getting-started-with-helio for more information.\n" + 
			"	--write= (optional): specifies a file in which the generated RDF will be written. This option can be used with the --cache flag.\n" + 
			"	--plugins (optional): specifies a folder in which Helio plugins are located, so they can be used by Helio.\n" + 
			"   --close (optional): specifies to Helio to shutdown the process after the data is generated, shutting down the asynchronous Data Sources.\n"+
			"	--h prints this information.\n"
			+ "\n"
			+ "The result of the previous command either creates a dump file (using the --write) or injects the generated RDF data into the selected cache (using the --cache). Notice that the Helio command does not finish after is executed, the reason are the asynchronous Data Sources that will be updated when required, and therefore, keep the process live. In order to specify Helio to shutdown after the generation of RDF the argument --close must be used.\n";
	
	public static void main(String[] args) {
		List<String> arguments = Arrays.asList(args);
		if(contains(arguments,HELP_ARGUMENT)) {
			System.out.println(MAN);
		}else if(contains(arguments,MAPPINGS_ARGUMENT)) {
			if(contains(arguments,PLUGINS_ARGUMENT)) {
				String pluginsFolder = findArgument(PLUGINS_ARGUMENT, arguments);
				HelioConfiguration.PLUGINS_FOLDER = pluginsFolder;
			}
			if(contains(arguments,CACHE_ARGUMENT)) {
				String cacheFile = findArgument(CACHE_ARGUMENT, arguments);
				loadRepositoryClass(cacheFile);
			}
			
			String mappingsFolder = findArgument(MAPPINGS_ARGUMENT, arguments);
			HelioMaterialiserMapping mapping = readMappingsFolder(mappingsFolder);
			
			
			
			System.out.println("Generating data");
			HelioMaterialiser helio = new HelioMaterialiser(mapping);
			helio.updateSynchronousSources();
			System.out.println("done!");
			if(contains(arguments, WRITE_ARGUMENT)) {
				System.out.println("writting data");
				String outputFile = findArgument(WRITE_ARGUMENT, arguments);	
				Model model = helio.getRDF();
				writeFile(outputFile, model);
			}
			if(contains(arguments,CLOSE_ARGUMENT)) {
				helio.close();
			}
		}else {
			System.out.println(MAN);
		}

	}
	
	private static boolean contains(List<String> arguments, String keyword) {
		Boolean contained =false;
		for(String argument:arguments) {
			if(argument.startsWith(keyword)) {
				contained = true;
				break;
			}
		}
		return contained;
	}

	private static void writeFile(String outputFile, Model model) {
		File targetFile = new File(outputFile);
	    try {
	    		if(targetFile.exists()) {
	    			targetFile.delete();
	    		}
	    		FileOutputStream out = new FileOutputStream(targetFile.getAbsoluteFile());
	    		try {
	    			Optional<RDFFormat> parsedFormatOptional = Rio.getParserFormatForFileName(outputFile);
	    			RDFFormat format = null;
	    			if(parsedFormatOptional.isPresent()) {
	    				format = parsedFormatOptional.get();
	    			}else {
	    				format = RDFFormat.TURTLE;
	    				System.out.println("Format not recognized, writting in TURTLE the data");
	    			}
	    			  Rio.write(model, out, format);
	    			}
	    			finally {
	    			  out.close();
	    			}
	    } catch (IOException e) {
	    	e.printStackTrace();
			System.out.println(e.toString());
		}
		
	}

	private static String findArgument(String key, List<String> arguments) {
		String argumentValue = null;
		for(String argument:arguments) {
			if(argument.startsWith(key)) {
				argumentValue = argument.replace(key, "");
				break;
			}
		}
		return argumentValue;
	}
	
	private static Repository loadRepositoryClass(String configuration) {
		File file = new File(configuration);
		String fileContent = readFile(file.getAbsolutePath());
		JsonObject repositoryJsonObject = (new Gson()).fromJson(fileContent, JsonObject.class);
		if(repositoryJsonObject.has("type")){
			String type = repositoryJsonObject.get("type").getAsString();
			if(type.equals("SPARQLRepository")) {
				instantiateSPARQLRepository(repositoryJsonObject);
			}else if(type.equals("HTTPRepository")) {
				instantiateHTTPRepository(repositoryJsonObject);
			}else if(type.equals("VirtuosoRepository")) {
				instantiateVirtuosoRepository(repositoryJsonObject);
			}else if(type.equals("ElasticsearchStore")){
				instantiateElasticsearchStore(repositoryJsonObject);
			}else {
				throw new IllegalArgumentException("Provided repository type is not supported");
			}
		}else {
			throw new IllegalArgumentException("Provided repository configuration does not have the mandatory key 'type'");
		}
		return null;
	}
	
	

	private static final String SPARQL_REPOSITORY_ARGUMENT = "sparql_endpoint";
	private static void instantiateSPARQLRepository(JsonObject repositoryJsonObject) {
		if(repositoryJsonObject.has(SPARQL_REPOSITORY_ARGUMENT)){
			String endpoint = repositoryJsonObject.get(SPARQL_REPOSITORY_ARGUMENT).getAsString();
			Repository repo = new SPARQLRepository(endpoint);
			HelioMaterialiser.HELIO_CACHE.changeRepository(repo);
		}else {
			throw new IllegalArgumentException("Provided repository configuration for SPARQLRepository lacks of mandatory key 'sparql_endpoint'");
		}
	}
	
	
	private static final String HTTPREPOSITORY_REPOSITORY_ARGUMENT_1 = "rdf4jServer";
	private static final String  HTTPREPOSITORY_REPOSITORY_ARGUMENT_2 = "repositoryID";
	private static void instantiateHTTPRepository(JsonObject repositoryJsonObject) {
		String endpoint = null;
		String repositoryID = null;
		if(repositoryJsonObject.has(HTTPREPOSITORY_REPOSITORY_ARGUMENT_1)){
			endpoint = repositoryJsonObject.get(HTTPREPOSITORY_REPOSITORY_ARGUMENT_1).getAsString();
		}else {
			throw new IllegalArgumentException("Provided repository configuration for SPARQLRepository lacks of mandatory key 'sparql_endpoint'");
		}
		if(repositoryJsonObject.has(HTTPREPOSITORY_REPOSITORY_ARGUMENT_2)){
			repositoryID = repositoryJsonObject.get(HTTPREPOSITORY_REPOSITORY_ARGUMENT_2).getAsString();
		}else {
			throw new IllegalArgumentException("Provided repository configuration for SPARQLRepository lacks of mandatory key 'repositoryID'");
		}
		if(endpoint!=null && repositoryID!=null) {
			Repository repo = new HTTPRepository(endpoint, repositoryID);
			HelioMaterialiser.HELIO_CACHE.changeRepository(repo);
		}
		
	}
	
	private static final String VIRTUOSO_REPOSITORY_REPOSITORY_ARGUMENT_1 = "endpoint";
	private static final String  VIRTUOSO_REPOSITORY_REPOSITORY_ARGUMENT_2 = "username";
	private static final String  VIRTUOSO_REPOSITORY_REPOSITORY_ARGUMENT_3 = "password";
	private static void instantiateVirtuosoRepository(JsonObject repositoryJsonObject) {
		String endpoint = null;
		String username = null;
		String password = null;
		if(repositoryJsonObject.has(VIRTUOSO_REPOSITORY_REPOSITORY_ARGUMENT_1)){
			endpoint = repositoryJsonObject.get(VIRTUOSO_REPOSITORY_REPOSITORY_ARGUMENT_1).getAsString();
		}else {
			throw new IllegalArgumentException("Provided repository configuration for VirtuosoRepository lacks of mandatory key 'endpoint'");
		}
		if(repositoryJsonObject.has(VIRTUOSO_REPOSITORY_REPOSITORY_ARGUMENT_2)){
			username = repositoryJsonObject.get(VIRTUOSO_REPOSITORY_REPOSITORY_ARGUMENT_2).getAsString();
		}else {
			throw new IllegalArgumentException("Provided repository configuration for VirtuosoRepository lacks of mandatory key 'username'");
		}
		if(repositoryJsonObject.has(VIRTUOSO_REPOSITORY_REPOSITORY_ARGUMENT_3)){
			password = repositoryJsonObject.get(VIRTUOSO_REPOSITORY_REPOSITORY_ARGUMENT_3).getAsString();
		}else {
			throw new IllegalArgumentException("Provided repository configuration for VirtuosoRepository lacks of mandatory key 'password'");
		}
		if(endpoint!=null && username!=null && password!=null) {
			Repository repo = new VirtuosoRepository(endpoint, username, password);
			HelioMaterialiser.HELIO_CACHE.changeRepository(repo);
		}
		
	}
	
	private static final String ELASTIC_REPOSITORY_REPOSITORY_ARGUMENT_1 = "hostname";
	private static final String ELASTIC_REPOSITORY_REPOSITORY_ARGUMENT_2 = "PORT";
	private static final String ELASTIC_REPOSITORY_REPOSITORY_ARGUMENT_3 = "clusterName";
	private static final String ELASTIC_REPOSITORY_REPOSITORY_ARGUMENT_4 = "index";
	private static void instantiateElasticsearchStore(JsonObject repositoryJsonObject) {
		String hostname = null;
		Integer port = null;
		String clusterName = null;
		String index = null; 
		if(repositoryJsonObject.has(ELASTIC_REPOSITORY_REPOSITORY_ARGUMENT_1)){
			hostname = repositoryJsonObject.get(ELASTIC_REPOSITORY_REPOSITORY_ARGUMENT_1).getAsString();
		}else {
			throw new IllegalArgumentException("Provided repository configuration for ElasticsearchStore lacks of mandatory key 'hostname'");
		}
		if(repositoryJsonObject.has(ELASTIC_REPOSITORY_REPOSITORY_ARGUMENT_2)){
			port = repositoryJsonObject.get(ELASTIC_REPOSITORY_REPOSITORY_ARGUMENT_2).getAsInt();
		}else {
			throw new IllegalArgumentException("Provided repository configuration for ElasticsearchStore lacks of mandatory key 'port'");
		}
		if(repositoryJsonObject.has(ELASTIC_REPOSITORY_REPOSITORY_ARGUMENT_3)){
			clusterName = repositoryJsonObject.get(ELASTIC_REPOSITORY_REPOSITORY_ARGUMENT_3).getAsString();
		}else {
			throw new IllegalArgumentException("Provided repository configuration for ElasticsearchStore lacks of mandatory key 'clusterName'");
		}
		if(repositoryJsonObject.has(ELASTIC_REPOSITORY_REPOSITORY_ARGUMENT_4)){
			index = repositoryJsonObject.get(ELASTIC_REPOSITORY_REPOSITORY_ARGUMENT_4).getAsString();
		}else {
			throw new IllegalArgumentException("Provided repository configuration for ElasticsearchStore lacks of mandatory key 'index'");
		}
		if(hostname!=null && port!=null && clusterName !=null && index!=null) {
			Repository repo = new SailRepository(new ElasticsearchStore(hostname, port, clusterName, index));
			HelioMaterialiser.HELIO_CACHE.changeRepository(repo);
		}
	}

	
	private static HelioMaterialiserMapping readMappingsFolder(String mappingsFolder) {
		HelioMaterialiserMapping mapping = new HelioMaterialiserMapping();
		MappingTranslator translator = new AutomaticTranslator();
		
		File mappingFolder = new File(mappingsFolder);
		File[] files = mappingFolder.listFiles();
		for(int index=0; index <files.length; index++) {
			File file = files[index];
			String mappingStr = readFile(file.getAbsolutePath());
			try {
				if(translator.isCompatible(mappingStr)) {	
					HelioMaterialiserMapping mappingAux = translator.translate(mappingStr);
					mapping.merge(mappingAux);
				}else {
					System.out.println("check the file: "+file);
				}
			}catch(Exception e) {
				System.out.println("check the file: "+file);
			}
		}
		return mapping;
	}
	
	 private static String readFile(String fileName) {
		 StringBuilder data = new StringBuilder();
			// 1. Read the file
			try {
				FileReader file = new FileReader(fileName);
				BufferedReader bf = new BufferedReader(file);
				// 2. Accumulate its lines in the data var
				bf.lines().forEach( line -> data.append(line).append("\n"));
				bf.close();
				file.close();
				// TODO: Opening and clossing im not sure is the best option 
			}catch(Exception e) {
				e.printStackTrace();
			} 
			return data.toString();
	 }

}
