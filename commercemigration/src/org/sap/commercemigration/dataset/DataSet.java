package org.sap.commercemigration.dataset;

import org.sap.commercemigration.dataset.impl.DefaultDataSet;

import java.util.Collections;
import java.util.List;

public interface DataSet {

	DataSet EMPTY = new DefaultDataSet(0, 0, Collections.emptyList(), Collections.emptyList());

	int getBatchId();

	int getColumnCount();

	List<List<Object>> getAllResults();

	Object getColumnValue(String column, List<Object> row);

	boolean isNotEmpty();

	boolean hasColumn(String column);
}
