package org.sap.commercemigration.filter;

import org.sap.commercemigration.context.MigrationContext;

import java.util.function.Predicate;

public interface DataCopyTableFilter {
	Predicate<String> filter(MigrationContext context);
}
