#!/usr/bin/env python3

from typing import List, Tuple, Callable
import argparse
import sys
from random import Random

parser = argparse.ArgumentParser(description='Tool for generating random data tables')
parser.add_argument('--num-rows', required=True, type=int, action='store', nargs=1, metavar='COUNT')
parser.add_argument('--col', required=True, action='append', nargs=2, metavar=('NAME', 'PATTERN'))
args = parser.parse_args()

def index_pattern(row_idx: int, seed: int):
    return str(row_idx + 1)

EU_COUNTRIES = ['Germany', 'Austria', 'France', 'Spain', 'Denmark']
MORE_COUNTRIES = EU_COUNTRIES + ['Russia', 'USA', 'Egypt', 'South-Korea']

def eu_countries_pattern(row_idx: int, seed: int):
    return EU_COUNTRIES[seed % len(EU_COUNTRIES)]

def more_countries_pattern(row_idx: int, seed: int):
    arr = ['Germany', 'Austria', 'France', 'Spain', 'Denmark']
    return arr[seed % len(arr)]

data_patterns = {
    'index': index_pattern,
    'eu_countries': eu_countries_pattern,
    'more_countries': more_countries_pattern,
}

def main(num_rows: int, cols: List[Tuple[str, str]]):
    # generate head row
    for i in range(0, len(cols)):
        sys.stdout.write(cols[i][0])

        if i + 1 < len(cols):
            sys.stdout.write(',')

    sys.stdout.write('\n')
    sys.stdout.flush()

    # generate value rows according to patterns
    rand = Random(1234556789) # TODO make configurable
    for j in range(0, num_rows):
        for i in range(0, len(cols)):

            data_pattern = data_patterns[cols[i][1]]
            seed = rand.randint(0, num_rows * len(cols) * 42) # NOTE just some integer
            value = (data_pattern)(j, seed)
            sys.stdout.write(value)

            if i + 1 < len(cols):
                sys.stdout.write(',')

        if j + 1 < num_rows:
            sys.stdout.write('\n')
        sys.stdout.flush()


main(args.num_rows[0], args.col)