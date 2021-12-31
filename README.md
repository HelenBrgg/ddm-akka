https://www.wdrmaus.de/blaubaer/filme/index.php5


# ddm-akka

## Compiling & running the system

Use Maven to build the project:

```sh
mvn package -f "pom.xml"
```

Then, extract the sample dataset:

```sh
cd data && unzip TPCH.zip && cd ..
```

Now the master system can be started like this:

```sh
java -Xmx3g -ea -cp target/ddm-akka-1.0.jar de.ddm.Main master
```

The system will run a while and shut down after it's finished. You  should receive output like this:

```sh
TODO
```

The actual results can be viewed in the incrementally-created `results.txt` and `results.csv` files.

## How does it work

The system activities can be broadly split in 3 phases. While these phases boot up and finish sequentially, they will soon overlap and run in parallel.

### 1. Reading and storing the dataset

After reading in the table headers, the table rows will be read in batches until all tables have been completely read.

In order to reduce memory usage, individual table columns will be stored in an optimized data structure called [`Column`](./src/main/java/de/ddm/structures).

When `Column` receives a value, it will compare it to all distinct values received so far and if it is already known, only store an index to it. These indicies will be used to read back the values in order. 

```
values stored in order:

    |"horse"|
    |"dog"  |
    |"dog"  |
    |"cat"  |
    |"horse"|

values stored in Column:

   distinct_values    indicies    readValue(index)
      |"horse"|         |0|        => "horse"
      |"dog"  |         |1|        => "dog"
      |"cat"  |         |1|        => "dog"
                        |2|        => "cat"
                        |0|        => "horse"

```

All data will be stored in a dedicated `LocalDataStorage` class. Currently, this is only an utility structure used by `DependencyMiner` - but in future it will be expanded to become a proper Akka actor. Each system will receive one such an actor to actively query and store data. 