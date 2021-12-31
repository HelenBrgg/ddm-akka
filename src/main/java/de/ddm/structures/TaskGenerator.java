package de.ddm.structures;

import java.util.*;

public class TaskGenerator {
    private LocalDataStorage dataStorage;
    private final int memoryBudget;

    private String tableNameA;
    private String tableNameB;
    private List<String> headerA;
    private List<String> headerB;

    private List<String> taskHeaderA = new ArrayList<>();
    private List<String> taskHeaderB = new ArrayList<>();
    private List<Task> generatedTasks = new ArrayList<>();

    private TaskGenerator(LocalDataStorage dataStorage, int memoryBudget, String tableNameA, String tableNameB){
        this.dataStorage = dataStorage;
        this.memoryBudget = memoryBudget;
        this.tableNameA = tableNameA;
        this.tableNameB = tableNameB;
        this.headerA = this.dataStorage.getHeader(tableNameA);
        this.headerB = this.dataStorage.getHeader(tableNameB);
    }

    private void generateTask(){
        this.generatedTasks.add(new Task(
            this.tableNameA, this.tableNameB,
            this.taskHeaderA, this.taskHeaderB));
    }

    private void runTableBGeneration(){
        int memoryUsed = 0;
        for (String columnNameB: this.headerB) {
            Column columnB = this.dataStorage.getColumn(tableNameB, columnNameB);

            // If we exceed memory budget, we want to create partial-table tasks
            // The `memoryUsed != 0` condition ensures that we always use at least 1 column.
            if (memoryUsed != 0 && memoryUsed + columnB.getMemorySize() > this.memoryBudget / 2) {
                this.generateTask();
                this.taskHeaderB = new ArrayList<>();
                memoryUsed = 0;
            }

            this.taskHeaderB.add(columnNameB);
            memoryUsed += columnB.getMemorySize();
        }
        this.generateTask();
    }

    private void runTableAGeneration(){
        int memoryUsed = 0;
        for (String columnNameA: this.headerA) {
            Column columnA = this.dataStorage.getColumn(tableNameA, columnNameA);

            // If we exceed memory budget, we want to create partial-table tasks.
            // The `memoryUsed != 0` condition ensures that we always use at least 1 column.
            if (memoryUsed != 0 && memoryUsed + columnA.getMemorySize() > this.memoryBudget / 2) {
                this.runTableBGeneration();
                this.taskHeaderA = new ArrayList<>();
                memoryUsed = 0;
            }

            this.taskHeaderA.add(columnNameA);
            memoryUsed += columnA.getMemorySize();
        }
        this.runTableBGeneration();
    }

    public static List<Task> run(LocalDataStorage dataStorage, int memoryBudget, String tableNameA, String tableNameB){
        TaskGenerator gen = new TaskGenerator(dataStorage, memoryBudget, tableNameA, tableNameB);
        gen.runTableAGeneration();
        return gen.generatedTasks;
    }
}
