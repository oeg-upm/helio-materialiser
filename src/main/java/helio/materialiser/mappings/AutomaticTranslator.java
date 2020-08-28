package helio.materialiser.mappings;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.reflections.Reflections;

import helio.framework.exceptions.MalformedMappingException;
import helio.framework.materialiser.MappingTranslator;
import helio.framework.materialiser.mappings.HelioMaterialiserMapping;

/**
 * This class implements a {@link MappingTranslator} that looks among all available {@link MappingTranslator} implementations to find a suitable one to translate a provided mapping
 * @author Andrea Cimmino
 *
 */
public class AutomaticTranslator implements MappingTranslator{

	private static Logger logger = LogManager.getLogger(AutomaticTranslator.class);
	private List<MappingTranslator> translators;
	
	/**
	 * This constructor initializes the {@link AutomaticTranslator}
	 */
	public AutomaticTranslator() {
		translators = new ArrayList<>();
		findMappingTranslators();
	}
	
	
	private void findMappingTranslators(){
		Reflections reflections = new Reflections("helio.materialiser.mappings");
		Set<Class<? extends MappingTranslator>> mappingTranslatorClasses = reflections.getSubTypesOf(MappingTranslator.class);
		
		for(Class<? extends MappingTranslator> clazz:mappingTranslatorClasses) {
			try {
				if(!clazz.getCanonicalName().equals("helio.materialiser.mappings.AutomaticTranslator")){
					MappingTranslator instance = clazz.newInstance();
					translators.add(instance);
				}
			} catch (Exception e) {
				logger.error(e.toString());
			}
		}	
		
	}
	

	@Override
	public Boolean isCompatible(String mappingContent) {
		Boolean result = false;
		for(int index=0; index < translators.size(); index++) {
			MappingTranslator translator = translators.get(index);
			if(translator.isCompatible(mappingContent)) {
				result = true;
				break;
			}
		}
		return result;
	}

	@Override
	public HelioMaterialiserMapping translate(String mappingContent) throws MalformedMappingException {
		HelioMaterialiserMapping outputMapping =  null;
		for(int index=0; index < translators.size(); index++) {
			MappingTranslator translator = translators.get(index);
			if(translator.isCompatible(mappingContent)) {
				outputMapping = translator.translate(mappingContent);
				break;
			}
		}
		if(outputMapping==null) {
			logger.warn("No suitable translator found for the provided mapping: \n"+mappingContent);
			outputMapping = new HelioMaterialiserMapping();
		}
		return outputMapping;
	}
	
	
	
}
