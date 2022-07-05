/*
 * Copyright: 2021 SAP SE or an SAP affiliate company and commerce-migration-toolkit contributors.
 * License: Apache-2.0
*/
package org.sap.commercemigration.context.validation;

import org.sap.commercemigration.context.MigrationContext;

public interface MigrationContextValidator {

	void validateContext(MigrationContext context);

}
