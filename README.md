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
