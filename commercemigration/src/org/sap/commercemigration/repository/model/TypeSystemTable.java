package org.sap.commercemigration.repository.model;

public class TypeSystemTable {
    private String typeCode;
    private String tableName;
    private String name;
    private String typeSystemName;
    private String auditTableName;
    private String propsTableName;
    private String typeSystemSuffix;
    private boolean typeSystemRelatedTable;

    public String getTypeCode() {
        return typeCode;
    }

    public void setTypeCode(String typeCode) {
        this.typeCode = typeCode;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getTypeSystemName() {
        return typeSystemName;
    }

    public void setTypeSystemName(String typeSystemName) {
        this.typeSystemName = typeSystemName;
    }

    public String getAuditTableName() {
        return auditTableName;
    }

    public void setAuditTableName(String auditTableName) {
        this.auditTableName = auditTableName;
    }

    public String getPropsTableName() {
        return propsTableName;
    }

    public void setPropsTableName(String propsTableName) {
        this.propsTableName = propsTableName;
    }

    public String getTypeSystemSuffix() {
        return typeSystemSuffix;
    }

    public void setTypeSystemSuffix(String typeSystemSuffix) {
        this.typeSystemSuffix = typeSystemSuffix;
    }

    public boolean isTypeSystemRelatedTable() {
        return typeSystemRelatedTable;
    }

    public void setTypeSystemRelatedTable(boolean typeSystemRelatedTable) {
        this.typeSystemRelatedTable = typeSystemRelatedTable;
    }
}
