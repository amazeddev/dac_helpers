#!/usr/bin/python3
import numpy as np
import pandas as pd
import sys

def check(name):

  hdf = pd.HDFStore('./.dac/data/{}.h5'.format(name),'r+')

  print(hdf.keys())

  for k in hdf.keys():
    data = hdf.get(k)
    print(k)
    print(data)
  hdf.close()

if __name__ == "__main__":
  name = sys.argv[1]
  check(name)