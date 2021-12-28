/*
 * Copyright: 2021 SAP SE or an SAP affiliate company and commerce-migration-toolkit contributors.
 * License: Apache-2.0
*/
package org.sap.commercemigration.dataset;

public interface DataColumn {

	String getColumnName();

	int getColumnType();

	int getPrecision();

	int getScale();

}
