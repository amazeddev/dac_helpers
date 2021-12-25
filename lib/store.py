#!/usr/bin/python3
import numpy as np
import pandas as pd
import sys
import json


from utils import connect

def store(work_name, results, name):
  df = pd.DataFrame()
  
  conn = connect()
  for _, col_id in enumerate(results):
    raw_res = conn("KVStore.Get", {"Key": col_id,"Base": False})
    col_gen = (e for e in raw_res["Data"])
    col = [np.nan if e else next(col_gen) for e in raw_res["Pres"]]
    df[raw_res["Name"]] = np.array(col)

  print(df)

  hdf = pd.HDFStore('./.dac/data/{}.h5'.format(name),'a')
  hdf.put("/{}".format(work_name), df, format='t', data_columns=True)
  hdf.close()

if __name__ == "__main__":
    work_name = sys.argv[1]
    print("work name:", work_name)
    raw_results = sys.argv[2]
    results = json.loads(raw_results)
    name = sys.argv[3]
    store(work_name, results, name)