#!/usr/bin/python3
from utils import connect
import pandas as pd
import numpy as np
import sys
import json
import uuid
import traceback as tb
from datetime import datetime

class Data():

  @staticmethod
  def check(name=None, key=None, columns=None):
    df = pd.DataFrame()
    with pd.HDFStore('./.dac/data/{}.h5'.format(name),'a') as hdf:
      keys = [k.split("/")[2] for k in hdf.keys() if k.split("/")[1] == key]
      for k in keys:
        col = hdf.select("/{}/{}".format(key, k), start=0, stop=10)
        df[k] = col
    
    print(df)
    hdf.close()


  @staticmethod
  def store(name=None, results=[], key=None):
    df = pd.DataFrame()
    
    conn = connect()
    for _, col_id in enumerate(results):
      raw_res = conn("KVStore.Get", {"Key": col_id})
      col_gen = (e for e in raw_res["Data"])
      col = [np.nan if e else next(col_gen) for e in raw_res["Pres"]]
      df[raw_res["Name"]] = np.array(col)

    with pd.HDFStore('./.dac/data/{}.h5'.format(name),'a') as hdf:
      for c in df.columns:
        hdf.put('/{}/{}'.format(key, c), df[c], format='t', data_columns=True)

    print(df.head(10))
        
    hdf.close()

  @staticmethod
  def remove(name=None, columns=[], key=None):
      
    with pd.HDFStore('./.dac/data/{}.h5'.format(name),'a') as hdf:
      keys = [k.split("/")[2] for k in hdf.keys() if k.split("/")[1] == key]
      if len(columns) == 0:
        for k in keys:
          hdf.remove('/{}/{}'.format(key, k))
      else:
        for c in columns:
          if c in keys:
            hdf.remove('/{}/{}'.format(key, c))

    print(columns)
        
    hdf.close()
    # TODO: free hdf5 space here?!

  @staticmethod
  def fetch(name=None, columns=None):
    
    # load the HDF5 data into a dataframe
    with pd.HDFStore('./.dac/data/{}.h5'.format(name), mode='r') as hdf:
      df = pd.DataFrame({c: hdf.get('/import/{}'.format(c)) for c in columns})

    conn = connect()
    resps = []
    for i, e in enumerate(df.columns):
      col = df.iloc[:10, i].values
      notnan = list(map(int, pd.isnull(col).tolist()))
      id = str(uuid.uuid4()).replace("-", "")
      res = conn("KVStore.Put", {"Key": id, "Value": {
        "Name": e,
        "Type": str(col.dtype),
        "Data": col[~(pd.isnull(col))].tolist(),
        "Pres": notnan
      },"Base": False})
      if res:
        resps.append({
          "id":id,
          "name":e
        })

    print(json.dumps({
      "resp": resps
    }))
  
  @staticmethod
  def imprt(name, path):
    csv = pd.read_csv(path)

    with pd.HDFStore('./.dac/data/{}.h5'.format(name),'a') as hdf:
      for i in csv.columns:
        hdf.append('import/{}'.format(i), csv[i])
        
    hdf.close()

if __name__ == "__main__":

  method_name = sys.argv[1]
  raw_args = sys.argv[2]
  args = json.loads(raw_args)

  errFile = open(".dac/errFile.txt", "a+")
  try:
    method = getattr(Data, method_name)
    method(**args)
  except Exception as e:
    errFile.write("%s\n" %datetime.now())
    errFile.write(''.join(tb.format_exception(None, e, e.__traceback__)))
    errFile.write("\n")
    
  errFile.close()