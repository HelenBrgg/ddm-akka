#!/usr/bin/env python3

# Aufgabe 1: Fuzzing
# 1. data/TPCH/*.csv tabelle einlesen
# 2. Zeilen (evtl. in zufälliger reihenfolge oder zyklisch) ausgeben, bis num_rows erreicht
# 3. mit X%-wahrscheinlichkeit Wert modifizieren
# 4. mit X%-wahrscheinlichkeit Zeilen mixen
#
# Benutzung: ./datagen --num-rows 100 --col A fuzz_customer 

# Aufgabe 2: Unix Sockets
# 1. --stream argument für die kommand-zeile
# 2. stdout auf einen Unix-Socket umleiten
# 
# Benutzung: ./datagen ... --stream table1.csv

from typing import List, Tuple, Callable
import argparse
import sys
from random import Random

parser = argparse.ArgumentParser(description='Tool for generating random data tables')
parser.add_argument('--num-rows', required=True, type=int, action='store', nargs=1, metavar='COUNT')
parser.add_argument('--col', required=True, action='append', nargs=2, metavar=('NAME', 'PATTERN'))
args = parser.parse_args()
print(args)

def index_pattern(row: int, col: int, seed: int):
    return str(row + 1)

EU_COUNTRIES = ['Germany', 'Austria', 'France', 'Spain', 'Denmark']
MORE_COUNTRIES = EU_COUNTRIES + ['Russia', 'USA', 'Egypt', 'South-Korea']
NULL_COUNTRIES = EU_COUNTRIES + MORE_COUNTRIES + ['']

def eu_countries_pattern(row: int, col: int, seed: int):
    return EU_COUNTRIES[seed % len(EU_COUNTRIES)]

def more_countries_pattern(row: int, col: int, seed: int):
    return MORE_COUNTRIES[seed % len(MORE_COUNTRIES)]

def null_countries_pattern(row: int, col: int, seed: int):
    return NULL_COUNTRIES[seed % len(NULL_COUNTRIES)]

data_patterns = {
    'index': index_pattern,
    'eu_countries': eu_countries_pattern,
    'more_countries': more_countries_pattern,
    'null_countries': null_countries_pattern
}

# ===== fuzzing =====

fuzz_tables = [] # TODO csv tabellen einlesen

def fuzz_pattern(table, row: int, col: int, seed: int):
    pass # TODO zelle wählen

for table in fuzz_tables:
    data_patterns['fuzz_' + table.name] = lambda row, col, seed: fuzz_pattern(table, row, col, seed)

# ===== main =====

if args.stream is None
    stream = sys.stdout
else:
    pass # TODO unix socket angeben

def main(num_rows: int, cols: List[Tuple[str, str]]):
    # generate head row
    for i in range(0, len(cols)):
        stream.write(cols[i][0])

        if i + 1 < len(cols):
            stream.write(',')

    stream.write('\n')
    stream.flush()

    # generate value rows according to patterns
    rand = Random(1234556789) # TODO make configurable
    for j in range(0, num_rows):

        seed = rand.randint(0, num_rows * len(cols) * 42) # NOTE just some integer

        for i in range(0, len(cols)):
            data_pattern = data_patterns[cols[i][1]]
            value = (data_pattern)(j, i, seed)
            stream.write(value)

            if i + 1 < len(cols):
                stream.write(',')

        if j + 1 < num_rows:
            stream.write('\n')
        stream.flush()

main(args.num_rows[0], args.col)