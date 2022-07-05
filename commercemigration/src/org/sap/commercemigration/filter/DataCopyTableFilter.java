/*
 * Copyright: 2021 SAP SE or an SAP affiliate company and commerce-migration-toolkit contributors.
 * License: Apache-2.0
*/
package org.sap.commercemigration.filter;

import org.sap.commercemigration.context.MigrationContext;

import java.util.function.Predicate;

public interface DataCopyTableFilter {
	Predicate<String> filter(MigrationContext context);
}
