package ddp.algo;

import java.util.stream.*;
import java.util.HashSet;
import java.util.HashMap;
import java.util.List;
import java.io.FileReader;

public class UnaryInclusion {
    public static class Dependency {
        public String columnX;
        public String columnY;

        public Dependency(String columnX, String columnY){
            this.columnX = columnX;
            this.columnY = columnY;
        }
    }

    private static HashMap<String, HashSet<String>> extractUnique(DataSource table) {
        HashMap<String, HashSet<String>> unique = new HashMap<>(); 
        table.streamDataPoints()
            .forEach(dataX -> {
                if (!unique.containsKey(dataX.column)) {
                    unique.put(dataX.column, new HashSet<>());
                }
                unique.get(dataX.column).add(dataX.value);
            });
        return unique;
    }

    public static List<Dependency> run(DataSource tableX, DataSource tableY) {
        HashMap<String, HashSet<String>> uniqueX = extractUnique(tableX);
        HashMap<String, HashSet<String>> uniqueY = extractUnique(tableY);
        
        return uniqueX.entrySet().stream()
            .flatMap(colX -> uniqueY.entrySet().stream()
                .filter(colY -> colY.getValue().containsAll(colX.getValue()))
                .map(colY -> new Dependency(colX.getKey(), colY.getKey())))
            .collect(Collectors.toList());
    }

    public static void main(String args[]) {
        if (args.length != 2) {
            System.out.printf("Usage: ./run CSV_X CSV_Y\n", args[0]);
        }

        try {
            DataSource sourceX = new DataSource(new FileReader(args[0]));
            DataSource sourceY = new DataSource(new FileReader(args[1]));

            List<Dependency> deps = run(sourceX, sourceY);
            for (Dependency dep : deps) {
                System.out.printf("%s ⊆ %s\n", dep.columnX, dep.columnY);
            }
        } catch (Exception e) {
            System.out.printf("Error: %s\n", e.toString());
        }
    }
}
