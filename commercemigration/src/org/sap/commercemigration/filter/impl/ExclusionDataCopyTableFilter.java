/*
 * Copyright: 2021 SAP SE or an SAP affiliate company and commerce-migration-toolkit contributors.
 * License: Apache-2.0
*/
package org.sap.commercemigration.filter.impl;

import com.google.common.base.Predicates;
import org.apache.commons.lang.StringUtils;
import org.sap.commercemigration.context.MigrationContext;
import org.sap.commercemigration.filter.DataCopyTableFilter;

import java.util.Set;
import java.util.function.Predicate;

public class ExclusionDataCopyTableFilter implements DataCopyTableFilter {

	@Override
	public Predicate<String> filter(MigrationContext context) {
		Set<String> excludedTables = context.getExcludedTables();
		if (excludedTables == null || excludedTables.isEmpty()) {
			return Predicates.alwaysTrue();
		}
		return p -> excludedTables.stream().noneMatch(e -> StringUtils.equalsIgnoreCase(e, p));
	}
}
