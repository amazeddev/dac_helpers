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
    df = pd.read_hdf('./.dac/data/{}.h5'.format(name), key, columns=columns)   
    print(df.head(10))

  @staticmethod
  def store(name=None, results=[], key=None):
    df = pd.DataFrame()
    
    conn = connect()
    for _, col_id in enumerate(results):
      raw_res = conn("KVStore.Get", {"Key": col_id})
      col_gen = (e for e in raw_res["Data"])
      col = [np.nan if e else next(col_gen) for e in raw_res["Pres"]]
      df[raw_res["Name"]] = np.array(col)

    hdf = pd.HDFStore('./.dac/data/{}.h5'.format(name),'a')
    hdf.put("/{}".format(key), df, format='t', data_columns=True)
    hdf.close()

  @staticmethod
  def fetch(name=None, columns=None):
    df = pd.read_hdf('./.dac/data/{}.h5'.format(name), 'import', columns=columns)

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

    hdf = pd.HDFStore('./.dac/data/{}.h5'.format(name),'a')
    hdf.put('import', csv, format='t', data_columns=True)
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