package de.ddm.actors.profiling;

import java.util.*;

public class LocalDataStorage {
    Map<String, List<String>> headerList = new HashMap<String, List<String>>();
    Map<String, List<List<String>>> contentList = new HashMap<String, List<List<String>>>();

    public LocalDataStorage() {}

    void addTable(String tableName, List<String> header) {
        this.headerList.put(tableName, header);
        this.contentList.put(tableName, new ArrayList<List<String>>());
    }

    List<String> getTableNames() {
        List<String> tableNames = new ArrayList<String>();
        for (String key : headerList.keySet()) {
            tableNames.add(key);
        }
        return tableNames;
    }

    List<String> getColumn(String tableName, String columnName) {
        List<List<String>> table = this.contentList.get(tableName);
        int index = headerList.get(tableName).indexOf(columnName);
        List<String> column = new ArrayList<String>();

        for (List<String> row : table) {
            column.add(row.get(index));
        }
        return column;
    }

    void addRow(String tableName, List<String> values) {
        this.contentList.get(tableName).add(values);
    }

    public static void main(String args[]) {
        LocalDataStorage store = new LocalDataStorage();
        
        store.addTable("1", Arrays.asList("A","B", "C", "D"));
        store.addRow("1", Arrays.asList("e","f", "g", "h"));
        store.addRow("1", Arrays.asList("i","j", "k", "l"));
        store.addRow("1", Arrays.asList("x","y", "z", "w"));

        store.addTable("1", Arrays.asList("A","B", "C", "D"));
        store.addRow("1", Arrays.asList("n","m", "l", "k"));

        System.out.println(store.getColumn("1", "B").toString());
    }
}
