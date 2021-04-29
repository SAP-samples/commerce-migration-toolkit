package org.sap.commercemigration.processors.impl;

import org.apache.commons.lang3.StringUtils;
import org.sap.commercemigration.context.CopyContext;
import org.sap.commercemigration.processors.MigrationPostProcessor;
import org.sap.commercemigration.repository.DataRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;

public class AdjustActiveTypeSystemPostProcessor implements MigrationPostProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(AdjustActiveTypeSystemPostProcessor.class.getName());

    private static final String CCV2_TS_MIGRATION_TABLE = "CCV2_TYPESYSTEM_MIGRATIONS";
    private static final String TYPESYSTEM_ADJUST_STATEMENT = "IF (EXISTS (SELECT * \n" +
            "  FROM INFORMATION_SCHEMA.TABLES \n" +
            "  WHERE TABLE_SCHEMA = '%s' \n" +
            "  AND TABLE_NAME = '%3$s'))\n" +
            "BEGIN\n" +
            "  UPDATE [%3$s] SET [state] = 'retired' WHERE 1=1;\n" +
            "  UPDATE [%3$s] SET [state] = 'current', [comment] = 'Updated by CMT' WHERE [name] = '%s';\n" +
            "END";

    @Override
    public void process(CopyContext context) {
        final DataRepository targetRepository = context.getMigrationContext().getDataTargetRepository();
        final String typeSystemName = targetRepository.getDataSourceConfiguration().getTypeSystemName();

        try (final Connection connection = targetRepository.getConnection()) {
            final PreparedStatement statement = connection.prepareStatement(String.format(TYPESYSTEM_ADJUST_STATEMENT,
                    targetRepository.getDataSourceConfiguration().getSchema(), typeSystemName, getMigrationsTableName(targetRepository)));

            statement.execute();

            LOG.info("Adjusted active type system to: " + typeSystemName);
        } catch (Exception e) {
            LOG.error("Error executing post processor", e);
        }
    }

    private String getMigrationsTableName(DataRepository repository) {
        return StringUtils.trimToEmpty(repository.getDataSourceConfiguration().getTablePrefix()).concat(CCV2_TS_MIGRATION_TABLE);
    }
}
