package helio.materialiser.run;

import java.net.URISyntaxException;

import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.UnsupportedRDFormatException;

import helio.framework.exceptions.MalformedMappingException;
import helio.framework.materialiser.mappings.HelioMaterialiserMapping;
import helio.materialiser.HelioMaterialiser;
import helio.materialiser.configuration.HelioConfiguration;
import helio.materialiser.mappings.RMLTranslator;

public class TestRML {
	
	
	private static final String MAPPING = "@prefix rr: <http://www.w3.org/ns/r2rml#>.\n" + 
			"@prefix rml: <http://semweb.mmlab.be/ns/rml#>.\n" + 
			"@prefix ex: <http://example.com/ns#>.\n" + 
			"@prefix ql: <http://semweb.mmlab.be/ns/ql#>.\n" + 
			"@prefix transit: <http://vocab.org/transit/terms/>.\n" + 
			"@prefix xsd: <http://www.w3.org/2001/XMLSchema#>.\n" + 
			"@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>.\n" + 
			"@base <http://example.com/ns#>.\n" + 
			"\n" + 
			"<#TransportMapping>\n" + 
			"  rml:logicalSource [\n" + 
			"    rml:source \"Transport.xml\" ;\n" + 
			"    rml:iterator \"/transport/bus\";\n" + 
			"    rml:referenceFormulation ql:XPath;\n" + 
			"  ];\n" + 
			"\n" + 
			"  rr:subjectMap [ \n" + 
			"    rr:template \"http://trans.example.com/{@id}\";\n" + 
			"    rr:class transit:Stop \n" + 
			"  ];\n" + 
			"\n" + 
			"  rr:predicateObjectMap [\n" + 
			"    rr:predicate transit:stop;\n" + 
			"    rr:objectMap [\n" + 
			"      rml:reference \"route/stop/@id\";\n" + 
			"      rr:datatype xsd:int \n" + 
			"    ] \n" + 
			"  ];\n" + 
			"\n" + 
			"  rr:predicateObjectMap [\n" + 
			"    rr:predicate rdfs:label;\n" + 
			"    rr:objectMap [\n" + 
			"      rml:reference \"route/stop\"\n" + 
			"    ]\n" + 
			"  ].";
	
	private static final String MAPPING_AUX = "@prefix ns0: <http://semweb.mmlab.be/ns/rml#> .\n" + 
			"@prefix rr: <http://www.w3.org/ns/r2rml#> .\n" + 
			"@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n" + 
			"@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n" + 
			"\n" + 
			"<http://example.com/ns#TransportMapping>\n" + 
			"  ns0:logicalSource [\n" + 
			"    ns0:source \"Transport.xml\" ;\n" + 
			"    ns0:iterator \"/transport/bus\" ;\n" + 
			"    ns0:referenceFormulation <http://semweb.mmlab.be/ns/ql#XPath>\n" + 
			"  ] ;\n" + 
			"  rr:subjectMap [\n" + 
			"    rr:template \"http://trans.example.com/{@id}\" ;\n" + 
			"    rr:class <http://vocab.org/transit/terms/Stop>\n" + 
			"  ] ;\n" + 
			"  rr:predicateObjectMap [\n" + 
			"    rr:predicate <http://vocab.org/transit/terms/stop> ;\n" + 
			"    rr:objectMap <http://example.com/ns#obm4>\n" + 
			"  ], [\n" + 
			"    rr:predicate rdfs:label ;\n" + 
			"    rr:objectMap [ ns0:reference \"route/stop\" ]\n" + 
			"  ] .\n" + 
			"\n" + 
			"<http://example.com/ns#obm4>\n" + 
			"  ns0:reference \"route/stop/@id\" ;\n" + 
			"  rr:datatype xsd:int .";
	
	public static void main(String[] args) throws MalformedMappingException, RDFHandlerException, UnsupportedRDFormatException, URISyntaxException   {
		RMLTranslator translator = new RMLTranslator();
		
		HelioMaterialiserMapping mapping = translator.translate(MAPPING_AUX);
		HelioMaterialiser materialiser = new HelioMaterialiser(mapping);
		
		materialiser.updateSynchronousSources();
		Rio.write(materialiser.getRDF(), System.out, HelioConfiguration.DEFAULT_BASE_URI, RDFFormat.TURTLE);
		materialiser.close();
	}

}
