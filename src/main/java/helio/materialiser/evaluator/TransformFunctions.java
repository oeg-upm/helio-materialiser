package helio.materialiser.evaluator;

import java.util.Base64;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.jsoup.Jsoup;

/**
 * This class provides a set of functions to transform {@link String} into other representations
 * @author Andrea Cimmino
 *
 */
public class TransformFunctions extends Functions {


	
	/**
	 * This method capitalizes the input value
	 * @param value a {@link String} input value
	 * @return a capitalized {@link String}
	 */
	public static String capitalize(String value) {
		return Character.toUpperCase(value.charAt(0)) + value.substring(1);
	}
	
	/**
	 * This method translates a {@link String} into a compatible URL, for instance transforms text like ' ' into URL special char %20
	 * @param value a {@link String} input value
	 * @return a valid {@link String} formated as a URL
	 */
	public static String escapeHtml3(String value) {
		return StringEscapeUtils.escapeHtml3(value);
	}
	
	/**
	 * This method translates a {@link String} into a compatible URL, for instance transforms text like 'á' into the character 'a&amp;acute'
	 * @param value a {@link String} input value
	 * @return a valid {@link String} formated as a URL
	 */
	public static String escapeHtml4(String value) {
		return StringEscapeUtils.escapeHtml4(value);
	}
	
	/**
	 * This method parses an HTML document
	 * @param value a {@link String} input value
	 * @return a {@link String} parsed HTML document
	 */
	public static String textFromHTML(String value) {
		return Jsoup.parse(value).text();
	}
	
	/**
	 * This method removes the accents from the input value
	 * @param value a {@link String} input value
	 * @return a {@link String} without accents
	 */
	public static String stripAccents(String value) {
		return StringUtils.stripAccents(value);
	}

	/**
	 * This method encodes the input value as a Base 64
	 * @param value a {@link String} input value
	 * @return a Base 64 {@link String} 
	 */
	public static String encodeB64(String value) {
		return Base64.getEncoder().encodeToString(value.getBytes());
	}
	
	/**
	 * This method decodes the input value from a Base 64
	 * @param value a Base 64 {@link String}  
	 * @return a decoded {@link String} 
	 */
	public static String decodeB64(String value) {
		byte [] barr = Base64.getDecoder().decode(value); 
		return new String(barr);
	}
	
	
	/**
	 * This method splits the given value using the provided regular expression
	 * @param value a {@link String} input value
	 * @param regex a {@link String} regular expression
	 * @return an array of {@link String} as result of splitting the input value
	 */
	public static String[] splitBy(String value, String regex) {
		return value.split(regex);
	}
	
	/**
	 * This method generates the hash code of the input value
	 * @param value a {@link String} input value
	 * @return a {@link String} hash code
	 */
	public static String hashCode(String value) {
		return String.valueOf(value.hashCode());
	}
	
}
