/*
 * Copyright: 2021 SAP SE or an SAP affiliate company and commerce-migration-toolkit contributors.
 * License: Apache-2.0
*/
package org.sap.commercemigration.filter.impl;

import com.google.common.base.Predicates;
import org.apache.commons.lang.StringUtils;
import org.sap.commercemigration.constants.CommercemigrationConstants;
import org.sap.commercemigration.context.MigrationContext;
import org.sap.commercemigration.filter.DataCopyTableFilter;

import java.util.Set;
import java.util.function.Predicate;

public class IncrementalDataCopyTableFilter implements DataCopyTableFilter {

	@Override
	public Predicate<String> filter(MigrationContext context) {
		if (!context.isIncrementalModeEnabled()) {
			return Predicates.alwaysTrue();
		}
		Set<String> incrementalTables = context.getIncrementalTables();
		if (incrementalTables == null || incrementalTables.isEmpty()) {
			throw new IllegalStateException("At least one table for incremental copy must be specified. Check property "
					+ CommercemigrationConstants.MIGRATION_DATA_INCREMENTAL_TABLES);
		}
		return p -> incrementalTables.stream().anyMatch(e -> StringUtils.equalsIgnoreCase(e, p));
	}
}
