package org.sap.commercemigration.repository;


import de.hybris.bootstrap.ddl.DataBaseProvider;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.model.Database;
import org.sap.commercemigration.dataset.DataSet;
import org.sap.commercemigration.profile.DataSourceConfiguration;
import org.sap.commercemigration.repository.model.TypeSystemTable;
import org.springframework.core.io.Resource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Set;

/**
 *
 */
public interface DataRepository {
    Database asDatabase();

    Database asDatabase(boolean reload);

    Set<String> getAllTableNames() throws Exception;

    Set<TypeSystemTable> getAllTypeSystemTables() throws Exception;

    boolean isAuditTable(String table) throws Exception;

    Set<String> getAllColumnNames(String table) throws Exception;

    DataSet getBatchWithoutIdentifier(String table, Set<String> allColumns, long batchSize, long offset) throws Exception;

    DataSet getBatchWithoutIdentifier(String table, Set<String> allColumns, long batchSize, long offset, Instant time) throws Exception;

    DataSet getBatchOrderedByColumn(String table, String column, Object start, long batchSize) throws Exception;

    DataSet getBatchOrderedByColumn(String table, String column, Object start, long batchSize, Instant time) throws Exception;

    DataSet getBatchMarkersOrderedByColumn(String table, String column, long batchSize) throws Exception;

    long getRowCount(String table) throws Exception;

    long getRowCountModifiedAfter(String table, Instant time) throws SQLException;

    DataSet getAll(String table) throws Exception;

    DataSet getAllModifiedAfter(String table, Instant time) throws Exception;

    DataSourceConfiguration getDataSourceConfiguration();

    int executeUpdateAndCommit(String updateStatement) throws Exception;

    void runSqlScript(final Resource resource);

    float getDatabaseUtilization() throws SQLException;

    int truncateTable(String table) throws Exception;

    void disableIndexesOfTable(String table) throws Exception;

    void enableIndexesOfTable(String table) throws SQLException;

    void dropIndexesOfTable(String table) throws SQLException;

    Platform asPlatform();

    Platform asPlatform(boolean reload);

    DataBaseProvider getDatabaseProvider();

    Connection getConnection() throws Exception;

    DataSource getDataSource();

    DataSet getBatchMarkersOrderedByColumn(String table, String column, long batchSize, Instant time) throws Exception;

    DataSet getUniqueColumns(String table) throws Exception;

    boolean validateConnection() throws Exception;
}
