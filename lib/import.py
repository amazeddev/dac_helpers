#!/usr/bin/python3
import numpy as np
import pandas as pd
import sys

def store(path, name):
  csv = pd.read_csv(path)

  hdf = pd.HDFStore('./.dac/data/{}.h5'.format(name),'a')
  hdf.put('import', csv, format='t', data_columns=True)
  hdf.close()

if __name__ == "__main__":
    path = sys.argv[1]
    name = sys.argv[2]
    store(path, name)