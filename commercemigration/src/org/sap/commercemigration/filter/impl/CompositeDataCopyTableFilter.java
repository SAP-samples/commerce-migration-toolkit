/*
 * Copyright: 2021 SAP SE or an SAP affiliate company and commerce-migration-toolkit contributors.
 * License: Apache-2.0
*/
package org.sap.commercemigration.filter.impl;

import org.sap.commercemigration.context.MigrationContext;
import org.sap.commercemigration.filter.DataCopyTableFilter;

import java.util.List;
import java.util.function.Predicate;

public class CompositeDataCopyTableFilter implements DataCopyTableFilter {

	private List<DataCopyTableFilter> filters;

	@Override
	public Predicate<String> filter(MigrationContext context) {
		return p -> filters.stream().allMatch(f -> f.filter(context).test(p));
	}

	public void setFilters(List<DataCopyTableFilter> filters) {
		this.filters = filters;
	}
}
