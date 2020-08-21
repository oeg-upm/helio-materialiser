package helio.materialiser.executors;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.rdf4j.rio.RDFFormat;

import com.google.gson.Gson;
import java.lang.reflect.Type;
import com.google.gson.reflect.TypeToken;

import helio.materialiser.HelioMaterialiser;

import com.google.gson.JsonArray;

public class ExecutableLinkRule {

	private static Logger logger = LogManager.getLogger(ExecutableLinkRule.class);

	private String sourceSubject;
	private String targetSubject;
	private String expression;
	private String sourceValues;
	private String targetValues;
	private Integer id;
	private String predicate;
	private String inversePredicate;
	
	public ExecutableLinkRule(Integer id, String sourceSubject, String targetSubject, String expression, String sourceValuesExtracted, String targetValuesExtracted, String predicate, String inversePredicate) {
		this.id = id;
		this.targetSubject = targetSubject;
		this.sourceSubject = sourceSubject;
		this.expression = expression;
		this.sourceValues = sourceValuesExtracted;
		this.targetValues = targetValuesExtracted;
		this.predicate = predicate;
		this.inversePredicate = inversePredicate;
	}


	public void performLinking() {
		Gson gson = new Gson();
		List<String> sourceDataReferences = extractDataReferences(true);
		JsonArray sourceDataValues = gson.fromJson(sourceValues, JsonArray.class);
		List<String> targetDataReferences = extractDataReferences(false);
		JsonArray targetDataValues = gson.fromJson(targetValues, JsonArray.class);
		
		if(sourceSubject!=null && targetSubject!=null && !sourceSubject.equals("null") && !targetSubject.equals("null")) {
			processSourceValues(sourceDataReferences,  sourceDataValues,  targetDataReferences,  targetDataValues,  gson);
		}else {
			logger.warn("Link rule "+expression+" found a null value for either the source subject ("+sourceSubject+") or the target subject ("+targetSubject+").");
		}
			
		
		
	}
	
	private void processSourceValues(List<String> sourceDataReferences, JsonArray sourceDataValues, List<String> targetDataReferences, JsonArray targetDataValues, Gson gson) {
		Type empMapType = new TypeToken<Map<String, String>>() {}.getType();
		// init expression with source values
				for(int index=0; index < sourceDataValues.size(); index++) {
					Map<String, String> sourcedataValuesMap = gson.fromJson(sourceDataValues.get(index).getAsJsonObject(), empMapType);
					String instantiatedSourceExpression = instantiateLinkingExpression(this.expression, sourceDataReferences, sourcedataValuesMap, true);
					if(instantiatedSourceExpression!=null) {
						// init expressions with target values
						processTargetValues(targetDataReferences , targetDataValues, gson, empMapType, instantiatedSourceExpression);
					}else {
						logger.error(getLogLine("An error ocurred instantiating the link rule ",expression,", specifically instantiating the source data references ",sourceDataReferences.toString()," using the data values ",sourceDataValues.toString()));
					}
				}
	}
	
	private void processTargetValues(List<String> targetDataReferences , JsonArray targetDataValues, Gson gson, Type empMapType, String instantiatedSourceExpression) {
		for(int counter=0; counter < targetDataValues.size(); counter++) {
			Map<String, String> targetdataValuesMap = gson.fromJson(targetDataValues.get(counter).getAsJsonObject(), empMapType);
			String instantiatedTargetExpression = instantiateLinkingExpression(instantiatedSourceExpression, targetDataReferences, targetdataValuesMap, false);
			if(instantiatedTargetExpression!=null) {
				// store
				linkdAndStore(instantiatedTargetExpression); 
			}else {
				logger.error(getLogLine("An error ocurred instantiating the link rule ",expression,", specifically instantiating the target data references ",targetDataReferences.toString()," using the data values ",targetDataValues.toString()));
			}
		}
	}
	
	private void linkdAndStore(String instantiatedTargetExpression) {
		Boolean linked = HelioMaterialiser.EVALUATOR.evaluatePredicate(instantiatedTargetExpression);
		if(linked) {
			StringBuilder builder = new StringBuilder();
			if(predicate!=null && !predicate.isEmpty() && !predicate.equals("null")) {
				builder.append("<").append(sourceSubject).append("> <").append(predicate).append("> <").append(targetSubject).append(">  .\n");
				HelioMaterialiser.HELIO_CACHE.addGraph(createGraphIdentifier(sourceSubject), builder.toString(), RDFFormat.NTRIPLES);
			}
			if(inversePredicate!=null && !inversePredicate.isEmpty() && !inversePredicate.equals("null")) {
				builder.append("<").append(targetSubject).append("> <").append(inversePredicate).append("> <").append(sourceSubject).append(">  .");
				HelioMaterialiser.HELIO_CACHE.addGraph(createGraphIdentifier(targetSubject), builder.toString(), RDFFormat.NTRIPLES);
			}
		}
	}
	
	
	private String createGraphIdentifier(String subject) {
		StringBuilder builder = new StringBuilder();
		builder.append(subject);
		if(subject.endsWith("/"))
			builder.append("/");
		builder.append("/links");
		return builder.toString();
	}
	
	private String getLogLine(String str1, String str2, String str3, String str4, String str5, String str6) {
		StringBuilder str = new StringBuilder();
		str.append(str1).append(str2).append(str3).append(str4).append(str5).append(str6);
		return str.toString();
	}
	
	private List<String> extractDataReferences(Boolean isSource) {
		// 1. Compile pattern to find identifiers
		String token = "T";
		if(isSource)
			token = "S";
		Pattern pattern = Pattern.compile(token+"\\(\\{[^\\}\\)]+\\}\\)");
		Matcher matcher = pattern.matcher(this.expression);
		// 2. Find identifiers with compiled matcher from pattern
		List<String> results = new ArrayList<>();
		while (matcher.find()) {
			String newIdentifier = matcher.group();
			// 2.1 Clean identifier found
			newIdentifier = newIdentifier.replace(token+"({", "").replace("})", "").trim();
			results.add(newIdentifier);
		}
		
		return results;
	}


	private String instantiateLinkingExpression(String expression, List<String> dataReferences, Map<String,String> dataValues, Boolean isSource) {
		String token = "T";
		if(isSource)
			token = "S";
		String expressionCopy =null;
		if(dataReferences.isEmpty()) {
			expressionCopy = expression;
		}else if (!dataReferences.isEmpty() && dataValues!=null && !dataValues.isEmpty()) {
			expressionCopy = expression;
			for(int index=0;index<dataReferences.size();index++) {
				String reference = dataReferences.get(index);
				String value = dataValues.get(reference);
				if(value!=null) {
					StringBuilder builder = new StringBuilder();
					builder.append(token).append("({").append(reference).append("})");
					StringBuilder instance = new StringBuilder();
					instance.append("'").append(dataValues.get(reference)).append("'");
					expressionCopy = expressionCopy.replace(builder.toString(), instance.toString());
				}else {
					expressionCopy =null;
					break;
				}
			}
		}
		return expressionCopy;
	}

	
	// Getters & Setters

	public String getSourceSubject() {
		return sourceSubject;
	}

	public void setSourceSubject(String sourceSubject) {
		this.sourceSubject = sourceSubject;
	}

	public String getTargetSubject() {
		return targetSubject;
	}

	public void setTargetSubject(String targetSubject) {
		this.targetSubject = targetSubject;
	}

	public String getExpression() {
		return expression;
	}

	public void setExpression(String expression) {
		this.expression = expression;
	}

	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}





	@Override
	public String toString() {
		return "ExecutableLinkRule [sourceSubject=" + sourceSubject + ", targetSubject=" + targetSubject
				+ ", expression=" + expression + ", sourceValues=" + sourceValues + ", targetValues=" + targetValues
				+ ", id=" + id + "]";
	}





	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((expression == null) ? 0 : expression.hashCode());
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		result = prime * result + ((sourceSubject == null) ? 0 : sourceSubject.hashCode());
		result = prime * result + ((sourceValues == null) ? 0 : sourceValues.hashCode());
		result = prime * result + ((targetSubject == null) ? 0 : targetSubject.hashCode());
		result = prime * result + ((targetValues == null) ? 0 : targetValues.hashCode());
		return result;
	}





	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ExecutableLinkRule other = (ExecutableLinkRule) obj;
		if (expression == null) {
			if (other.expression != null)
				return false;
		} else if (!expression.equals(other.expression))
			return false;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		if (sourceSubject == null) {
			if (other.sourceSubject != null)
				return false;
		} else if (!sourceSubject.equals(other.sourceSubject))
			return false;
		if (sourceValues == null) {
			if (other.sourceValues != null)
				return false;
		} else if (!sourceValues.equals(other.sourceValues))
			return false;
		if (targetSubject == null) {
			if (other.targetSubject != null)
				return false;
		} else if (!targetSubject.equals(other.targetSubject))
			return false;
		if (targetValues == null) {
			if (other.targetValues != null)
				return false;
		} else if (!targetValues.equals(other.targetValues))
			return false;
		return true;
	}



	

	

}
