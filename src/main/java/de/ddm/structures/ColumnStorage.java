package de.ddm.structures;

import java.util.*;

// TODO we should optimize this to store data in column-order as well.
public class ColumnStorage {
    Map<String, List<String>> headerList = new HashMap<String, List<String>>();
    Map<String, List<List<String>>> contentList = new HashMap<String, List<List<String>>>();

    public ColumnStorage() {}

    public void addTable(String tableName, List<String> header) {
        this.headerList.put(tableName, header);
        this.contentList.put(tableName, new ArrayList<List<String>>());
    }

    public List<String> getTableNames() {
        List<String> tableNames = new ArrayList<String>();
        for (String key : headerList.keySet()) {
            tableNames.add(key);
        }
        return tableNames;
    }

    public List<String> getHeader(String tableName) {
        return this.headerList.get(tableName);
    }

    public List<String> getColumn(String tableName, String columnName) {
        List<List<String>> table = this.contentList.get(tableName);
        int index = headerList.get(tableName).indexOf(columnName);

        List<String> column = new ArrayList<String>(table.size());
        for (List<String> row : table) {
            column.add(row.get(index));
        }
        return column;
    }

    public void addRow(String tableName, List<String> values) {
        this.contentList.get(tableName).add(values);
    }

    public static void main(String args[]) {
        ColumnStorage store = new ColumnStorage();
        
        store.addTable("1", Arrays.asList("A","B", "C", "D"));
        store.addRow("1", Arrays.asList("e","f", "g", "h"));
        store.addRow("1", Arrays.asList("i","j", "k", "l"));
        store.addRow("1", Arrays.asList("x","y", "z", "w"));

        store.addTable("1", Arrays.asList("A","B", "C", "D"));
        store.addRow("1", Arrays.asList("n","m", "l", "k"));

        System.out.println(store.getColumn("1", "B").toString());
    }
}
