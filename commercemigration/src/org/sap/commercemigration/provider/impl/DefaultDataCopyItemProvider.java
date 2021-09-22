package org.sap.commercemigration.provider.impl;

import com.google.common.collect.Sets;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.sap.commercemigration.TableCandidate;
import org.sap.commercemigration.TypeSystemTable;
import org.sap.commercemigration.constants.CommercemigrationConstants;
import org.sap.commercemigration.context.CopyContext;
import org.sap.commercemigration.context.MigrationContext;
import org.sap.commercemigration.filter.DataCopyTableFilter;
import org.sap.commercemigration.provider.CopyItemProvider;
import org.sap.commercemigration.repository.DataRepository;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class DefaultDataCopyItemProvider implements CopyItemProvider {

    public static final String SN_SUFFIX = "sn";
    private static final String LP_SUFFIX = "lp";

    private static final String[] TYPE_SYSTEM_RELATED_TYPES = new String[]{"atomictypes", "attributeDescriptors", "collectiontypes", "composedtypes", "enumerationvalues", "maptypes"};
    private final Comparator<TableCandidate> tableCandidateComparator = (o1, o2) -> o1.getCommonTableName().compareToIgnoreCase(o2.getCommonTableName());
    private DataCopyTableFilter dataCopyTableFilter;

    private static boolean shouldMigrateAuditTable(final MigrationContext context, final String auditTableName) {
        return context.isAuditTableMigrationEnabled() && StringUtils.isNotEmpty(auditTableName);
    }

    @Override
    public Set<CopyContext.DataCopyItem> get(MigrationContext context) throws Exception {
        Set<TableCandidate> sourceTablesCandidates = getSourceTableCandidates(context);
        Set<TableCandidate> targetTablesCandidates = getTargetTableCandidates(context);

        Sets.SetView<TableCandidate> sourceTables = Sets.intersection(sourceTablesCandidates, targetTablesCandidates);
        Set<TableCandidate> sourceTablesToMigrate = sourceTables.stream().filter(t -> dataCopyTableFilter.filter(context).test(t.getCommonTableName())).collect(Collectors.toSet());

        return createCopyItems(context, sourceTablesToMigrate, targetTablesCandidates.stream().collect(Collectors.toMap(t -> t.getCommonTableName().toLowerCase(), t -> t)));
    }

    @Override
    public Set<TableCandidate> getSourceTableCandidates(MigrationContext context) throws Exception {
        return getTableCandidates(context, context.getDataSourceRepository());
    }

    @Override
    public Set<TableCandidate> getTargetTableCandidates(MigrationContext context) throws Exception {
        return getAllTableCandidates(context);
    }

    private Set<TableCandidate> getAllTableCandidates(MigrationContext context) throws Exception {
        final DataRepository targetRepository = context.getDataTargetRepository();
        String prefix = targetRepository.getDataSourceConfiguration().getTablePrefix();

        return targetRepository.getAllTableNames().stream()
                .filter(n -> prefix == null || StringUtils.startsWithIgnoreCase(n, prefix))
                .map(n -> StringUtils.removeStartIgnoreCase(n, prefix))
                .filter(n -> !isNonMatchingTypesystemTable(targetRepository, n))
                .map(n -> createTableCandidate(targetRepository, n))
                .collect(Collectors.toCollection(() -> new TreeSet<>(tableCandidateComparator)));
    }

    private boolean isNonMatchingTypesystemTable(DataRepository repository, String tableName) {
        boolean isTypesystemTable = false;
        if (!StringUtils.endsWithIgnoreCase(tableName, SN_SUFFIX)) {
            isTypesystemTable = Arrays.stream(TYPE_SYSTEM_RELATED_TYPES).anyMatch(t -> StringUtils.startsWithIgnoreCase(tableName, t));
        }
        if (isTypesystemTable) {
            String additionalSuffix = getAdditionalSuffix(tableName);
            String tableNameWithoutAdditionalSuffix = getTableNameWithoutAdditionalSuffix(tableName, additionalSuffix);
            String typeSystemSuffix = repository.getDataSourceConfiguration().getTypeSystemSuffix();
            return !StringUtils.endsWithIgnoreCase(tableNameWithoutAdditionalSuffix, typeSystemSuffix);
        }
        return false;
    }

    private Set<TableCandidate> getTableCandidates(MigrationContext context, DataRepository repository) throws Exception {
        Set<String> allTableNames = repository.getAllTableNames();
        Set<TableCandidate> tableCandidates = new TreeSet<>(tableCandidateComparator);

        //add meta tables
        tableCandidates.add(createTableCandidate(repository, CommercemigrationConstants.DEPLOYMENTS_TABLE));
        tableCandidates.add(createTableCandidate(repository, "aclentries"));
        tableCandidates.add(createTableCandidate(repository, "configitems"));
        tableCandidates.add(createTableCandidate(repository, "numberseries"));
        tableCandidates.add(createTableCandidate(repository, "metainformations"));

        //add tables listed in "ydeployments"
        Set<TypeSystemTable> allTypeSystemTables = repository.getAllTypeSystemTables();
        allTypeSystemTables.forEach(t -> {
            tableCandidates.add(createTableCandidate(repository, t.getTableName()));

            String propsTableName = t.getPropsTableName();

            if (StringUtils.isNotEmpty(propsTableName)) {
                tableCandidates.add(createTableCandidate(repository, t.getPropsTableName()));
            }

            TableCandidate lpTable = createTableCandidate(repository, t.getTableName() + LP_SUFFIX);

            if (allTableNames.contains(lpTable.getFullTableName())) {
                tableCandidates.add(lpTable);
            }

            if (shouldMigrateAuditTable(context, t.getAuditTableName())) {
                TableCandidate auditTable = createTableCandidate(repository, t.getAuditTableName());

                if (allTableNames.contains(auditTable.getFullTableName())) {
                    tableCandidates.add(auditTable);
                }
            }
        });

        // custom tables
        if (CollectionUtils.isNotEmpty(context.getCustomTables())) {
            tableCandidates.addAll(context.getCustomTables().stream().map(t -> createTableCandidate(repository, t)).collect(Collectors.toSet()));
        }

        return tableCandidates;
    }

    private TableCandidate createTableCandidate(DataRepository repository, String tableName) {
        TableCandidate candidate = new TableCandidate();
        String additionalSuffix = getAdditionalSuffix(tableName);
        String tableNameWithoutAdditionalSuffix = getTableNameWithoutAdditionalSuffix(tableName, additionalSuffix);
        String baseTableName = getTableNameWithoutTypeSystemSuffix(tableNameWithoutAdditionalSuffix, repository.getDataSourceConfiguration().getTypeSystemSuffix());
        boolean isTypeSystemRelatedTable = isTypeSystemRelatedTable(baseTableName);
        candidate.setCommonTableName(baseTableName + additionalSuffix);
        candidate.setTableName(tableName);
        candidate.setFullTableName(repository.getDataSourceConfiguration().getTablePrefix() + tableName);
        candidate.setAdditionalSuffix(additionalSuffix);
        candidate.setBaseTableName(baseTableName);
        candidate.setTypeSystemRelatedTable(isTypeSystemRelatedTable);
        return candidate;
    }


    private boolean isTypeSystemRelatedTable(String tableName) {
        return Arrays.stream(TYPE_SYSTEM_RELATED_TYPES).anyMatch(tableName::equalsIgnoreCase);
    }

    private String getAdditionalSuffix(String tableName) {
        if (StringUtils.endsWithIgnoreCase(tableName, LP_SUFFIX)) {
            return LP_SUFFIX;
        } else {
            return StringUtils.EMPTY;
        }
    }

    private String getTableNameWithoutTypeSystemSuffix(String tableName, String suffix) {
        return StringUtils.removeEnd(tableName, suffix);
    }

    private String getTableNameWithoutAdditionalSuffix(String tableName, String suffix) {
        return StringUtils.removeEnd(tableName, suffix);
    }

    private Set<CopyContext.DataCopyItem> createCopyItems(MigrationContext context, Set<TableCandidate> sourceTablesToMigrate, Map<String, TableCandidate> targetTablesToMigrate) {
        Set<CopyContext.DataCopyItem> copyItems = new HashSet<>();
        for (TableCandidate sourceTableToMigrate : sourceTablesToMigrate) {
            String targetTableKey = sourceTableToMigrate.getCommonTableName().toLowerCase();
            if (targetTablesToMigrate.containsKey(targetTableKey)) {
                TableCandidate targetTableToMigrate = targetTablesToMigrate.get(targetTableKey);
                copyItems.add(createCopyItem(context, sourceTableToMigrate, targetTableToMigrate));
            } else {
                throw new IllegalStateException("Target table must exists");
            }
        }
        return copyItems;
    }

    private CopyContext.DataCopyItem createCopyItem(MigrationContext context, TableCandidate sourceTable, TableCandidate targetTable) {
        String sourceTableName = sourceTable.getFullTableName();
        String targetTableName = targetTable.getFullTableName();
        CopyContext.DataCopyItem dataCopyItem = new CopyContext.DataCopyItem(sourceTableName, targetTableName);
        addColumnMappingsIfNecessary(context, sourceTable, dataCopyItem);
        return dataCopyItem;
    }

    private void addColumnMappingsIfNecessary(MigrationContext context, TableCandidate sourceTable, CopyContext.DataCopyItem dataCopyItem) {
        if (sourceTable.getCommonTableName().equalsIgnoreCase(CommercemigrationConstants.DEPLOYMENTS_TABLE)) {
            String sourceTypeSystemName = context.getDataSourceRepository().getDataSourceConfiguration().getTypeSystemName();
            String targetTypeSystemName = context.getDataTargetRepository().getDataSourceConfiguration().getTypeSystemName();
            //Add mapping to override the TypeSystemName value in target table
            if (!sourceTypeSystemName.equalsIgnoreCase(targetTypeSystemName)) {
                dataCopyItem.getColumnMap().put("TypeSystemName", targetTypeSystemName);
            }
        }
    }

    public void setDataCopyTableFilter(DataCopyTableFilter dataCopyTableFilter) {
        this.dataCopyTableFilter = dataCopyTableFilter;
    }
}
