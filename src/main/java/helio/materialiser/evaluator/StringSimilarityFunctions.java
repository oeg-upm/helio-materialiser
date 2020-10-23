package helio.materialiser.evaluator;

import info.debatty.java.stringsimilarity.Cosine;
import info.debatty.java.stringsimilarity.Jaccard;
import info.debatty.java.stringsimilarity.JaroWinkler;
import info.debatty.java.stringsimilarity.MetricLCS;
import info.debatty.java.stringsimilarity.NGram;
import info.debatty.java.stringsimilarity.NormalizedLevenshtein;
import info.debatty.java.stringsimilarity.RatcliffObershelp;
import info.debatty.java.stringsimilarity.SorensenDice;

/**
 * This class provides a set of functions to compare {@link String} using fuzzy functions
 * @author Andrea Cimmino
 *
 */
public class StringSimilarityFunctions extends Functions {

	private StringSimilarityFunctions() {
		super();
	}
	/**
	 * This function applies the <a href="https://github.com/tdebatty/java-string-similarity#normalized-levenshtein">normalized Levenshtein</a> distance between the provided strings
	 * @param elem1 a {@link String}
	 * @param elem2 a {@link String}
	 * @return a similarity score normalized between [0,1], 1 means that the arguments are equals, 0 are different.
	 */
	public static double levenshtein(String elem1, String elem2) {
		NormalizedLevenshtein lev = new NormalizedLevenshtein();
		return lev.similarity(elem1, elem2);
	}
	
	/**
	 * This function applies the <a href="https://github.com/tdebatty/java-string-similarity#cosine-similarity">Cosine</a> distance between the provided strings 
	 * @param elem1 a {@link String}
	 * @param elem2 a {@link String}
	 * @return a similarity score normalized between [0,1], 1 means that the arguments are equals, 0 are different.
	 */
	public static double cosine(String elem1, String elem2) {
		Cosine cosine = new Cosine();
		return cosine.similarity(elem1, elem2);
	}
	/**
	 * This function applies the <a href="https://github.com/tdebatty/java-string-similarity#metric-longest-common-subsequence">MetricLCS</a> distance between the provided strings 
	 * @param elem1 a {@link String}
	 * @param elem2 a {@link String}
	 * @return a similarity score normalized between [0,1], 1 means that the arguments are equals, 0 are different.
	 */
	public static double metricLCS(String elem1, String elem2) {
		MetricLCS lcs = new MetricLCS();
		return lcs.distance(elem1, elem2);
	}
	
	/**
	 * This function applies the <a href="https://github.com/tdebatty/java-string-similarity#jaro-winkler">JaroWinkler</a> distance between the provided strings 
	 * @param elem1 a {@link String}
	 * @param elem2 a {@link String}
	 * @return a similarity score normalized between [0,1], 1 means that the arguments are equals, 0 are different.
	 */
	public static double jaroWinkler(String elem1, String elem2) {
		JaroWinkler jw = new JaroWinkler();
		return jw.similarity(elem1, elem2);
	}
	
	/**
	 * This function applies the <a href="https://github.com/tdebatty/java-string-similarity#ratcliff-obershelp">RatcliffObershelp</a> distance between the provided strings 
	 * @param elem1 a {@link String}
	 * @param elem2 a {@link String}
	 * @return a similarity score normalized between [0,1], 1 means that the arguments are equals, 0 are different.
	 */
	public static double ratcliffObershelp(String elem1, String elem2) {
		RatcliffObershelp ro = new RatcliffObershelp();
		return ro.similarity(elem1, elem2);
	}
	
	
	/**
	 * This function applies the <a href="https://github.com/tdebatty/java-string-similarity#n-gram">2-Gram</a> distance between the provided strings 
	 * @param elem1 a {@link String}
	 * @param elem2 a {@link String}
	 * @return a similarity score normalized between [0,1], 1 means that the arguments are equals, 0 are different.
	 */
	public static double TwoGram(String elem1, String elem2) {
		NGram ngram = new NGram(2);
		return ngram.distance(elem1, elem2);
	}
	
	/**
	 * This function applies the <a href="https://github.com/tdebatty/java-string-similarity#n-gram">3-Gram</a> distance between the provided strings 
	 * @param elem1 a {@link String}
	 * @param elem2 a {@link String}
	 * @return a similarity score normalized between [0,1], 1 means that the arguments are equals, 0 are different.
	 */
	public static double ThreeGram(String elem1, String elem2) {
		NGram ngram = new NGram(3);
		return ngram.distance(elem1, elem2);
	}
	
	/**
	 * This function applies the <a href="https://github.com/tdebatty/java-string-similarity#n-gram">4-Gram</a> distance between the provided strings 
	 * @param elem1 a {@link String}
	 * @param elem2 a {@link String}
	 * @return a similarity score normalized between [0,1], 1 means that the arguments are equals, 0 are different.
	 */
	public static double FourGram(String elem1, String elem2) {
		NGram ngram = new NGram(4);
		return ngram.distance(elem1, elem2);
	}
	
	/**
	 * This function applies the <a href="https://github.com/tdebatty/java-string-similarity#n-gram">5-Gram</a> distance between the provided strings 
	 * @param elem1 a {@link String}
	 * @param elem2 a {@link String}
	 * @return a similarity score normalized between [0,1], 1 means that the arguments are equals, 0 are different.
	 */
	public static double FiveGram(String elem1, String elem2) {
		NGram ngram = new NGram(5);
		return ngram.distance(elem1, elem2);
	}
	
	/**
	 * This function applies the <a href="https://github.com/tdebatty/java-string-similarity#jaccard-index">Jaccard</a> distance between the provided strings 
	 * @param elem1 a {@link String}
	 * @param elem2 a {@link String}
	 * @return a similarity score normalized between [0,1], 1 means that the arguments are equals, 0 are different.
	 */
	public static double jaccard(String elem1, String elem2) {
		Jaccard jaccard= new Jaccard();
		return jaccard.distance(elem1, elem2);
	}
	
	/**
	 * This function applies the <a href="https://github.com/tdebatty/java-string-similarity#sorensen-dice-coefficient">Sorensen-Dice coefficient</a> distance between the provided strings 
	 * @param elem1 a {@link String}
	 * @param elem2 a {@link String}
	 * @return a similarity score normalized between [0,1], 1 means that the arguments are equals, 0 are different.
	 */
	public static double sorensen(String elem1, String elem2) {
		SorensenDice sorensen= new SorensenDice();
		return sorensen.distance(elem1, elem2);
	}
}
