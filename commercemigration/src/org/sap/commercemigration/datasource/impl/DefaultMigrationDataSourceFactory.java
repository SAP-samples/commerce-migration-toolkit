package org.sap.commercemigration.datasource.impl;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.sap.commercemigration.profile.DataSourceConfiguration;

import javax.sql.DataSource;

public class DefaultMigrationDataSourceFactory extends AbstractMigrationDataSourceFactory {

    //TODO: resource leak: DataSources are never closed
    @Override
    public DataSource create(DataSourceConfiguration dataSourceConfiguration) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(dataSourceConfiguration.getConnectionString());
        config.setDriverClassName(dataSourceConfiguration.getDriver());
        config.setUsername(dataSourceConfiguration.getUserName());
        config.setPassword(dataSourceConfiguration.getPassword());
//        config.setAccessToUnderlyingConnectionAllowed(true);
        config.setMaximumPoolSize(dataSourceConfiguration.getMaxActive());
//        dataSource.setMaxIdle(dataSourceConfiguration.getMaxIdle());
        config.setMinimumIdle(dataSourceConfiguration.getMinIdle());
        config.setRegisterMbeans(true);
        return new HikariDataSource(config);
    }

}
