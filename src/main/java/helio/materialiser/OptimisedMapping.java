package helio.materialiser;

import java.util.ArrayList;
import java.util.List;

import helio.framework.materialiser.mappings.DataSource;
import helio.framework.materialiser.mappings.HelioMaterialiserMapping;
import helio.framework.materialiser.mappings.RuleSet;

/**
 * This class is an alternative representation of a {@link HelioMaterialiserMapping} that generates the RDF more efficiently
 * @author Andrea Cimmino
 *
 */
public class OptimisedMapping {
	   
		private DataSource dataSource;
		private List<RuleSet> ruleSets;
	   
		/**
	    *  This constructor initializes the {@link OptimisedMapping}
	    */
		public OptimisedMapping() {
			ruleSets = new ArrayList<>();
		}
		
		/**
		 * This constructor initializes the {@link OptimisedMapping}
		 * @param dataSource a valid {@link DataSource} object
		 * @param ruleSets a set of {@link RuleSet} that are related to the provided {@link DataSource}
		 */
		public OptimisedMapping(DataSource dataSource, List<RuleSet> ruleSets) {
			this.dataSource = dataSource;
			this.ruleSets = ruleSets;
		}

		public DataSource getDataSource() {
			return dataSource;
		}

		public void setDataSource(DataSource dataSource) {
			this.dataSource = dataSource;
		}

		public List<RuleSet> getRuleSets() {
			return ruleSets;
		}

		public void setRuleSets(List<RuleSet> ruleSets) {
			this.ruleSets = ruleSets;
		}
		
	}