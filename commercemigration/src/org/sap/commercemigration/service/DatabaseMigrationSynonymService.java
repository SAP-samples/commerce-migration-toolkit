package org.sap.commercemigration.service;

import org.sap.commercemigration.repository.DataRepository;

public interface DatabaseMigrationSynonymService {

    /**
     * CCv2 Workaround: ccv2 builder does not support prefixes yet.
     * creating synonym on ydeployments -> prefix_yeployments
     * creating synonym on attributedescriptors -> prefix_attributedescriptors.
     *
     * @param repository
     * @param prefix
     * @throws Exception
     */
    void recreateSynonyms(DataRepository repository, String prefix) throws Exception;
}
