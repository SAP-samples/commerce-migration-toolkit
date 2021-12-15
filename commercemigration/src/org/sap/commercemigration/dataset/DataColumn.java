package org.sap.commercemigration.dataset;

public interface DataColumn {

	String getColumnName();

	int getColumnType();

	int getPrecision();

	int getScale();

}
