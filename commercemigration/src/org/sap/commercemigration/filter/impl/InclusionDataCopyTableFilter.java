package org.sap.commercemigration.filter.impl;

import com.google.common.base.Predicates;
import org.apache.commons.lang.StringUtils;
import org.sap.commercemigration.context.MigrationContext;
import org.sap.commercemigration.filter.DataCopyTableFilter;

import java.util.Set;
import java.util.function.Predicate;

public class InclusionDataCopyTableFilter implements DataCopyTableFilter {

	@Override
	public Predicate<String> filter(MigrationContext context) {
		Set<String> includedTables = context.getIncludedTables();
		if (includedTables == null || includedTables.isEmpty()) {
			return Predicates.alwaysTrue();
		}
		return p -> includedTables.stream().anyMatch(e -> StringUtils.equalsIgnoreCase(e, p));

	}
}
