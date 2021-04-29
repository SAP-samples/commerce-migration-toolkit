package org.sap.commercemigration.context.validation;

import org.sap.commercemigration.context.MigrationContext;

public interface MigrationContextValidator {

    void validateContext(MigrationContext context);

}
