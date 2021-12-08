package ddp.algo;

import java.util.stream.*;
import java.util.HashSet;
import java.util.HashMap;
import java.util.List;
import java.io.FileReader;
import lombok.AllArgsConstructor;
import lombok.Getter;

public class UnaryInclusion {
    @AllArgsConstructor
    @Getter
    public static class Dependency {
        public String columnX;
        public String columnY;

        @Override
        public String toString() {
            return this.columnX + " ⊆  " + this.columnY;
        }
    }

    private static HashMap<String, HashSet<String>> extractUnique(DataSource table) {
        HashMap<String, HashSet<String>> unique = new HashMap<>(); 
        table.streamDataPoints()
            .forEach(dataPoint -> {
                if (!unique.containsKey(dataPoint.getColumn())) {
                    unique.put(dataPoint.getColumn(), new HashSet<>());
                }
                unique.get(dataPoint.getColumn()).add(dataPoint.getValue());
            });
        return unique;
    }

    public static List<Dependency> run(DataSource tableX, DataSource tableY) {
        HashMap<String, HashSet<String>> uniqueX = extractUnique(tableX);
        HashMap<String, HashSet<String>> uniqueY = extractUnique(tableY);

        return uniqueX.entrySet().stream()
            .flatMap(colX -> uniqueY.entrySet().stream()
                .filter(colY -> colY.getValue().size() <= colY.getValue().size() && colY.getValue().containsAll(colX.getValue()))
                .map(colY -> new Dependency(colX.getKey(), colY.getKey())))
            .collect(Collectors.toList());
    }

    public static void main(String args[]) {
        if (args.length != 2) {
            System.out.printf("Usage: ./run CSV_X CSV_Y\n", args[0]);
        }

        try {
            System.out.println("Reading...");
            DataSource sourceX = new DataSource(new FileReader(args[0]));
            DataSource sourceY = new DataSource(new FileReader(args[1]));

            System.out.println("Running...");
            List<Dependency> deps1 = run(sourceX, sourceY);
            List<Dependency> deps2 = run(sourceY, sourceX);
            for (Dependency dep : deps1) {
                System.out.printf("%s ⊆ %s\n", dep.columnX, dep.columnY);
            }
            for (Dependency dep : deps2) {
                System.out.printf("%s ⊆ %s\n", dep.columnX, dep.columnY);
            }
        } catch (Exception e) {
            System.out.printf("Error: %s\n", e.toString());
        }
    }
}
