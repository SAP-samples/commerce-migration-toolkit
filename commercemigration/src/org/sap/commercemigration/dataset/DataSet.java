package org.sap.commercemigration.dataset;

import com.microsoft.sqlserver.jdbc.ISQLServerBulkData;
import org.sap.commercemigration.dataset.impl.DefaultDataSet;

import java.util.Collections;
import java.util.List;

public interface DataSet {

    DataSet EMPTY = new DefaultDataSet(0, Collections.EMPTY_LIST, Collections.EMPTY_LIST);

    int getColumnCount();

    List<List<Object>> getAllResults();

    Object getColumnValue(String column, List<Object> row);

    boolean isNotEmpty();

    boolean hasColumn(String column);

    ISQLServerBulkData toSQLServerBulkData();
}
