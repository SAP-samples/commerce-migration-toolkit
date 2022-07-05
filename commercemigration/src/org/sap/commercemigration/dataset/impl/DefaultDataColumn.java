/*
 * Copyright: 2021 SAP SE or an SAP affiliate company and commerce-migration-toolkit contributors.
 * License: Apache-2.0
*/
package org.sap.commercemigration.dataset.impl;

import org.sap.commercemigration.dataset.DataColumn;

public class DefaultDataColumn implements DataColumn {

	private final String name;
	private final int type;
	private final int precision;
	private final int scale;

	public DefaultDataColumn(String name, int type, int precision, int scale) {
		this.name = name;
		this.type = type;
		this.precision = precision;
		this.scale = scale;
	}

	@Override
	public String getColumnName() {
		return name;
	}

	@Override
	public int getColumnType() {
		return type;
	}

	@Override
	public int getPrecision() {
		return precision;
	}

	@Override
	public int getScale() {
		return scale;
	}
}
