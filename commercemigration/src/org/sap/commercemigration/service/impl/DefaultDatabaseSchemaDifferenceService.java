package org.sap.commercemigration.service.impl;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import de.hybris.platform.servicelayer.config.ConfigurationService;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Table;
import org.sap.commercemigration.TableCandidate;
import org.sap.commercemigration.constants.CommercemigrationConstants;
import org.sap.commercemigration.context.MigrationContext;
import org.sap.commercemigration.filter.DataCopyTableFilter;
import org.sap.commercemigration.provider.CopyItemProvider;
import org.sap.commercemigration.repository.DataRepository;
import org.sap.commercemigration.service.DatabaseMigrationReportStorageService;
import org.sap.commercemigration.service.DatabaseSchemaDifferenceService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DefaultDatabaseSchemaDifferenceService implements DatabaseSchemaDifferenceService {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultDatabaseSchemaDifferenceService.class);

    private DataCopyTableFilter dataCopyTableFilter;
    private DatabaseMigrationReportStorageService databaseMigrationReportStorageService;
    private CopyItemProvider copyItemProvider;
    private ConfigurationService configurationService;

    @Override
    public String generateSchemaDifferencesSql(MigrationContext context) throws Exception {
        final int maxStageMigrations = context.getMaxTargetStagedMigrations();
        Set<String> stagingPrefixes = findStagingPrefixes(context);
        if (stagingPrefixes.size() > maxStageMigrations) {
            Database databaseModelWithChanges = getDatabaseModelWithChanges4TableDrop(context);
            return context.getDataTargetRepository().asPlatform().getDropTablesSql(databaseModelWithChanges, true);
        } else {
            Database databaseModelWithChanges = getDatabaseModelWithChanges4TableCreation(context);
            return context.getDataTargetRepository().asPlatform().getAlterTablesSql(databaseModelWithChanges);
        }
    }


    @Override
    public void executeSchemaDifferencesSql(MigrationContext context, String sql) throws Exception {

        if (!context.isSchemaMigrationEnabled()) {
            throw new RuntimeException("Schema migration is disabled. Check property:" + CommercemigrationConstants.MIGRATION_SCHEMA_ENABLED);
        }

        Platform platform = context.getDataTargetRepository().asPlatform();
        boolean continueOnError = false;
        Connection connection = platform.borrowConnection();
        try {
            platform.evaluateBatch(connection, sql, continueOnError);
            LOG.info("Executed the following sql to change the schema:\n" + sql);
            writeReport(context, sql);
        } catch (Exception e) {
            throw new RuntimeException("Could not execute Schema Diff Script", e);
        } finally {
            platform.returnConnection(connection);
        }
    }

    @Override
    public void executeSchemaDifferences(MigrationContext context) throws Exception {
        executeSchemaDifferencesSql(context, generateSchemaDifferencesSql(context));
    }

    private Set<String> findDuplicateTables(MigrationContext migrationContext) {
        try {
            Set<String> stagingPrefixes = findStagingPrefixes(migrationContext);
            final Set<String> targetSet = migrationContext.getDataTargetRepository().getAllTableNames();
            return targetSet.stream().filter(t -> stagingPrefixes.stream().anyMatch(p -> StringUtils.startsWithIgnoreCase(t, p))).collect(Collectors.toSet());
        } catch (Exception e) {
            LOG.error("Error occurred while trying to find duplicate tables", e);
        }
        return Collections.EMPTY_SET;
    }

    private Set<String> findStagingPrefixes(MigrationContext context) throws Exception {
        final String currentSystemPrefix = configurationService.getConfiguration().getString("db.tableprefix");
        final String currentMigrationPrefix = context.getDataTargetRepository().getDataSourceConfiguration().getTablePrefix();
        final Set<String> targetSet = context.getDataTargetRepository().getAllTableNames();
        String deploymentsTable = CommercemigrationConstants.DEPLOYMENTS_TABLE;
        final Set<String> detectedPrefixes = targetSet.stream()
                .filter(t -> t.toLowerCase().endsWith(deploymentsTable))
                .filter(t -> !StringUtils.equalsIgnoreCase(t, currentSystemPrefix + deploymentsTable))
                .filter(t -> !StringUtils.equalsIgnoreCase(t, currentMigrationPrefix + deploymentsTable))
                .map(t -> StringUtils.removeEndIgnoreCase(t, deploymentsTable))
                .collect(Collectors.toSet());
        return detectedPrefixes;

    }

    private Database getDatabaseModelWithChanges4TableDrop(MigrationContext context) {
        Set<String> duplicateTables = findDuplicateTables(context);
        Database database = context.getDataTargetRepository().asDatabase(true);
        //clear tables and add only the ones to be removed
        Table[] tables = database.getTables();
        Stream.of(tables).forEach(t -> {
            database.removeTable(t);
        });
        duplicateTables.forEach(t -> {
            Table table = ObjectUtils.defaultIfNull(database.findTable(t), new Table());
            table.setName(t);
            database.addTable(table);
        });
        return database;
    }

    protected Database getDatabaseModelWithChanges4TableCreation(MigrationContext migrationContext) throws Exception {
        SchemaDifferenceResult differenceResult = getDifference(migrationContext);
        if (!differenceResult.hasDifferences()) {
            return migrationContext.getDataTargetRepository().asDatabase();
        }
        SchemaDifference targetDiff = differenceResult.getTargetSchema();
        Database database = targetDiff.getDatabase();

        //add missing tables in target
        if (migrationContext.isAddMissingTablesToSchemaEnabled()) {
            List<TableKeyPair> missingTables = targetDiff.getMissingTables();
            for (TableKeyPair missingTable : missingTables) {
                Table tableClone = (Table) differenceResult.getSourceSchema().getDatabase().findTable(missingTable.getLeftName(), false).clone();
                tableClone.setName(missingTable.getRightName());
                tableClone.setCatalog(migrationContext.getDataTargetRepository().getDataSourceConfiguration().getCatalog());
                tableClone.setSchema(migrationContext.getDataTargetRepository().getDataSourceConfiguration().getSchema());
                database.addTable(tableClone);
            }
        }

        //add missing columns in target
        if (migrationContext.isAddMissingColumnsToSchemaEnabled()) {
            ListMultimap<TableKeyPair, String> missingColumnsInTable = targetDiff.getMissingColumnsInTable();
            for (TableKeyPair missingColumnsTable : missingColumnsInTable.keySet()) {
                List<String> columns = missingColumnsInTable.get(missingColumnsTable);
                for (String missingColumn : columns) {
                    Table missingColumnsTableModel = differenceResult.getSourceSchema().getDatabase().findTable(missingColumnsTable.getLeftName(), false);
                    Column columnClone = (Column) missingColumnsTableModel.findColumn(missingColumn, false).clone();
                    Table table = database.findTable(missingColumnsTable.getRightName(), false);
                    Preconditions.checkState(table != null, "Data inconsistency: Table must exist.");
                    table.addColumn(columnClone);
                }
            }
        }

        //remove superfluous tables in target
        if (migrationContext.isRemoveMissingTablesToSchemaEnabled()) {
            throw new UnsupportedOperationException("not yet implemented");
        }

        //remove superfluous columns in target
        if (migrationContext.isRemoveMissingColumnsToSchemaEnabled()) {
            ListMultimap<TableKeyPair, String> superfluousColumnsInTable = differenceResult.getSourceSchema().getMissingColumnsInTable();
            for (TableKeyPair superfluousColumnsTable : superfluousColumnsInTable.keySet()) {
                List<String> columns = superfluousColumnsInTable.get(superfluousColumnsTable);
                for (String superfluousColumn : columns) {
                    Table table = database.findTable(superfluousColumnsTable.getLeftName(), false);
                    Preconditions.checkState(table != null, "Data inconsistency: Table must exist.");
                    Column columnToBeRemoved = table.findColumn(superfluousColumn, false);
                    //remove indices in case column is part of one
                    Stream.of(table.getIndices()).filter(i -> i.hasColumn(columnToBeRemoved)).forEach(i -> table.removeIndex(i));
                    table.removeColumn(columnToBeRemoved);
                }
            }
        }
        return database;
    }

    protected void writeReport(MigrationContext migrationContext, String differenceSql) {
        try {
            String fileName = String.format("schemaChanges-%s.sql", LocalDateTime.now().getNano());
            databaseMigrationReportStorageService.store(fileName, new ByteArrayInputStream(differenceSql.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) {
            LOG.error("Error executing writing diff report", e);
        }
    }

    @Override
    public SchemaDifferenceResult getDifference(MigrationContext migrationContext) throws Exception {
        try {
            LOG.info("reading source database model ...");
            migrationContext.getDataSourceRepository().asDatabase(true);
            LOG.info("reading target database model ...");
            migrationContext.getDataTargetRepository().asDatabase(true);

            LOG.info("compute source diff");
            Set<TableCandidate> targetTableCandidates = copyItemProvider.getTargetTableCandidates(migrationContext);
            SchemaDifference sourceSchemaDifference = computeDiff(migrationContext, migrationContext.getDataTargetRepository(), migrationContext.getDataSourceRepository(), targetTableCandidates);
            LOG.info("compute target diff");
            Set<TableCandidate> sourceTableCandidates = copyItemProvider.getSourceTableCandidates(migrationContext);
            SchemaDifference targetSchemaDifference = computeDiff(migrationContext, migrationContext.getDataSourceRepository(), migrationContext.getDataTargetRepository(), sourceTableCandidates);
            SchemaDifferenceResult schemaDifferenceResult = new SchemaDifferenceResult(sourceSchemaDifference, targetSchemaDifference);
            LOG.info("Diff finished. Differences detected: " + schemaDifferenceResult.hasDifferences());

            return schemaDifferenceResult;
        } catch (Exception e) {
            throw new RuntimeException("Error computing schema diff", e);
        }
    }

    protected String getSchemaDifferencesAsJson(SchemaDifferenceResult schemaDifferenceResult) {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        return gson.toJson(schemaDifferenceResult);
    }

    protected SchemaDifference computeDiff(MigrationContext context, DataRepository leftRepository, DataRepository rightRepository, Set<TableCandidate> leftCandidates) {
        SchemaDifference schemaDifference = new SchemaDifference(rightRepository.asDatabase(), rightRepository.getDataSourceConfiguration().getTablePrefix());
        Set<TableCandidate> leftDatabaseTables = getTables(context, leftRepository, leftCandidates);
        for (TableCandidate leftCandidate : leftDatabaseTables) {
            Table leftTable = leftRepository.asDatabase().findTable(leftCandidate.getFullTableName(), false);
            if (leftTable == null) {
                throw new RuntimeException(String.format("Table %s in DB %s cannot be found, but should exists", leftCandidate.getFullTableName(), leftRepository.getDataSourceConfiguration().getConnectionString()));
            }
            String rightTableName = translateTableName(leftRepository, rightRepository, leftCandidate);
            Table rightTable = rightRepository.asDatabase().findTable(rightTableName, false);
            if (rightTable == null) {
                schemaDifference.getMissingTables().add(new TableKeyPair(leftTable.getName(), rightTableName));
            } else {
                Column[] leftTableColumns = leftTable.getColumns();
                for (Column leftTableColumn : leftTableColumns) {
                    if (rightTable.findColumn(leftTableColumn.getName(), false) == null) {
                        schemaDifference.getMissingColumnsInTable().put(new TableKeyPair(leftTable.getName(), rightTable.getName()), leftTableColumn.getName());
                    }
                }
            }
        }
        return schemaDifference;
    }

    private String translateTableName(DataRepository leftRepository, DataRepository rightRepository, TableCandidate leftCandidate) {
        String translatedTableName = rightRepository.getDataSourceConfiguration().getTablePrefix() + leftCandidate.getBaseTableName();
        if (leftCandidate.isTypeSystemRelatedTable()) {
            translatedTableName += rightRepository.getDataSourceConfiguration().getTypeSystemSuffix();
        }
        return translatedTableName + leftCandidate.getAdditionalSuffix();
    }

    private Set<TableCandidate> getTables(MigrationContext context, DataRepository repository, Set<TableCandidate> candidates) {
        return candidates.stream()
                .filter(c -> dataCopyTableFilter.filter(context).test(c.getCommonTableName()))
                .collect(Collectors.toSet());
    }

    public void setDataCopyTableFilter(DataCopyTableFilter dataCopyTableFilter) {
        this.dataCopyTableFilter = dataCopyTableFilter;
    }

    public void setDatabaseMigrationReportStorageService(DatabaseMigrationReportStorageService databaseMigrationReportStorageService) {
        this.databaseMigrationReportStorageService = databaseMigrationReportStorageService;
    }

    public void setConfigurationService(ConfigurationService configurationService) {
        this.configurationService = configurationService;
    }

    public void setCopyItemProvider(CopyItemProvider copyItemProvider) {
        this.copyItemProvider = copyItemProvider;
    }

    public static class SchemaDifferenceResult {
        private SchemaDifference sourceSchema;
        private SchemaDifference targetSchema;

        public SchemaDifferenceResult(SchemaDifference sourceSchema, SchemaDifference targetSchema) {
            this.sourceSchema = sourceSchema;
            this.targetSchema = targetSchema;
        }

        public SchemaDifference getSourceSchema() {
            return sourceSchema;
        }

        public SchemaDifference getTargetSchema() {
            return targetSchema;
        }

        public boolean hasDifferences() {
            boolean hasMissingTargetTables = getTargetSchema().getMissingTables().size() > 0;
            boolean hasMissingColumnsInTargetTable = getTargetSchema().getMissingColumnsInTable().size() > 0;
            boolean hasMissingSourceTables = getSourceSchema().getMissingTables().size() > 0;
            boolean hasMissingColumnsInSourceTable = getSourceSchema().getMissingColumnsInTable().size() > 0;
            return hasMissingTargetTables || hasMissingColumnsInTargetTable || hasMissingSourceTables || hasMissingColumnsInSourceTable;
        }
    }

    public static class SchemaDifference {

        private Database database;
        private String prefix;

        private List<TableKeyPair> missingTables = new ArrayList<>();
        private ListMultimap<TableKeyPair, String> missingColumnsInTable = ArrayListMultimap.create();

        public SchemaDifference(Database database, String prefix) {
            this.database = database;
            this.prefix = prefix;

        }

        public Database getDatabase() {
            return database;
        }

        public String getPrefix() {
            return prefix;
        }

        public List<TableKeyPair> getMissingTables() {
            return missingTables;
        }

        public ListMultimap<TableKeyPair, String> getMissingColumnsInTable() {
            return missingColumnsInTable;
        }
    }

    public static class TableKeyPair {
        private String leftName;
        private String rightName;

        public TableKeyPair(String leftName, String rightName) {
            this.leftName = leftName;
            this.rightName = rightName;
        }

        public String getLeftName() {
            return leftName;
        }

        public String getRightName() {
            return rightName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TableKeyPair that = (TableKeyPair) o;
            return leftName.equals(that.leftName) &&
                    rightName.equals(that.rightName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(leftName, rightName);
        }
    }

}
