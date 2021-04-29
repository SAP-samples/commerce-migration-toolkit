/*
 * [y] hybris Platform
 *
 * Copyright (c) 2018 SAP SE or an SAP affiliate company. All rights reserved.
 *
 * This software is the confidential and proprietary information of SAP
 * ("Confidential Information"). You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms of the
 * license agreement you entered into with SAP.
 */
package de.hybris.platform.core;

import de.hybris.bootstrap.ddl.PropertiesLoader;

import java.util.Objects;


public class TenantPropertiesLoader implements PropertiesLoader {
    private final Tenant tenant;

    public TenantPropertiesLoader(final Tenant tenant) {
        Objects.requireNonNull(tenant);
        this.tenant = tenant;
    }

    @Override
    public String getProperty(final String key) {
        return tenant.getConfig().getParameter(key);
    }

    @Override
    public String getProperty(final String key, final String defaultValue) {
        return tenant.getConfig().getString(key, defaultValue);
    }
}