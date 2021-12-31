package de.ddm.structures;

import lombok.AllArgsConstructor;
import lombok.Getter;
import java.util.List;

@AllArgsConstructor
@Getter
public class Task {
    private final String tableNameA;
    private final String tableNameB;
    private final List<String> columnNamesA;
    private final List<String> columnNamesB;

    @Override
    public String toString(){
        return String.format("(%s%s,%s%s)", this.tableNameA, this.columnNamesA, this.tableNameB, this.columnNamesB);
    }
}