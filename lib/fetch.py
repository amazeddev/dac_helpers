#!/usr/bin/python3
import numpy as np
import pandas as pd
import sys
import json
import uuid

from utils import connect

def fetch(targets, name):
  df = pd.read_hdf('./.dac/data/{}.h5'.format(name), 'import', columns=targets)

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

if __name__ == "__main__":
  raw_targets = sys.argv[1]
  targets = json.loads(raw_targets)
  name = sys.argv[2]
  fetch(targets, name)