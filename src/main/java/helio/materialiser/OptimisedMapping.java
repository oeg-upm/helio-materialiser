package helio.materialiser;

import java.util.ArrayList;
import java.util.List;

import helio.framework.materialiser.mappings.DataSource;
import helio.framework.materialiser.mappings.RuleSet;

public class OptimisedMapping {
	   
		private DataSource dataSource;
		private List<RuleSet> ruleSets;
	   
		public OptimisedMapping() {
			ruleSets = new ArrayList<>();
		}
		
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