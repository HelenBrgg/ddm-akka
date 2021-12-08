https://www.wdrmaus.de/blaubaer/filme/index.php5


# ddm-akka

Generating random tables:

```
./datagen/datagen.py --num_rows 40 --col A countries --col B more_countries > data.csv
```

Running the unary inclusion algo:

```
java -cp target/ddm-akka-1.0.jar ddp.algo.UnaryInclusion exampleA.csv exampleB.csv
```

Before running the Master system, it's important to extract the example data first:

```
cd data && unzip TPCH.zip
```

Now you can run the master system like this:

```
java -cp target/ddm-akka-1.0.jar de.ddm.Main master
```
