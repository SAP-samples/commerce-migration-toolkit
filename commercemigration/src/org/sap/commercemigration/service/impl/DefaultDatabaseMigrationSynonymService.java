package org.sap.commercemigration.service.impl;

import org.sap.commercemigration.constants.CommercemigrationConstants;
import org.sap.commercemigration.repository.DataRepository;
import org.sap.commercemigration.service.DatabaseMigrationSynonymService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultDatabaseMigrationSynonymService implements DatabaseMigrationSynonymService {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultDatabaseMigrationSynonymService.class);

    private static final String YDEPLOYMENTS = CommercemigrationConstants.DEPLOYMENTS_TABLE;
    private static final String ATTRDESCRIPTORS = "attributedescriptors";


    @Override
    public void recreateSynonyms(DataRepository repository, String prefix) throws Exception {
        recreateSynonym(repository, YDEPLOYMENTS, prefix);
        recreateSynonym(repository, ATTRDESCRIPTORS, prefix);
    }

    private void recreateSynonym(DataRepository repository, String table, String actualPrefix) throws Exception {
        LOG.info("Creating Synonym for {} on {}{}", table, actualPrefix, table);
        repository.executeUpdateAndCommit(String.format("DROP SYNONYM IF EXISTS %s", table));
        repository.executeUpdateAndCommit(String.format("CREATE SYNONYM %s FOR %s%s", table, actualPrefix, table));
    }
}
