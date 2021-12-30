#!/usr/bin/python3
from utils import connect
import numpy as np
import pandas as pd
import sys
import json
import importlib
import uuid
import traceback as tb
from datetime import datetime

def high(f, data):
  res = f(data)
  return res

class Chain():

  def __init__(self, name, id):
    self.data = {}
    self.conn = connect()
    self.output = {
      "name": name,
      "id": id,
    }
    self.columns = {}

  def get(self, target=None, base=False):
    for t in target:
      raw_res = self.conn("KVStore.Get", {"Key": t})
      col_gen = (e for e in raw_res["Data"])
      col = [np.nan if e else next(col_gen) for e in raw_res["Pres"]]
      self.data[raw_res["Name"]] = np.array(col)
      self.columns[t] = raw_res["Name"]
    return self

  def pipe(self, func_expr=None, target=None, result=None, kwargs=None, type=None):
    data = [self.data[self.columns[t]] for t in target] if len(target) > 1 else self.data[self.columns[target[0]]]

    kwa = kwargs if kwargs else {}
    rest = None
    res = None

    if type == "lambda":
      func = eval(func_expr)
      res = func(data)
    elif type == "standard":
      module, func_name = func_expr.rsplit(".", 1)
      func = getattr(importlib.import_module("functions." + module), func_name)
      res, rest = func(data, **kwa)

    # one target (string) & one response (not nested list)
    if len(target) == 1 and isinstance(res, np.ndarray):
      if result and isinstance(result, str):
        self.data[result] = res
      else:
        self.data[self.columns[target[0]]] = res
    # many targets & one result
    elif len(target) > 1 and not any(isinstance(i, list) for i in res):
      self.data[result] = res
      for t in target:
        self.data.pop(self.columns[t])
    # one target & many results
    else:
      if result and len(result) == len(res):
        for i, r in enumerate(result):
          self.data[r] = res[i]
      else:
        for i, r in enumerate(rest):
          self.data[str(r)] = res[i]
      for t in target:
        self.data.pop(self.columns[t])
        
    return self

  def set(self):
    resps = []
    for key, value in self.data.items():
      notnan = list(map(int, pd.isnull(value).tolist()))

      id = str(uuid.uuid4()).replace("-", "")
      res = self.conn("KVStore.Put", {"Key": id, "Value": {
        "Type": str(value.dtype),
        "Name": key,
        "Data": value[~(pd.isnull(value))].tolist(),
        "Pres": notnan
      }}, False)

      if res:
        resps.append({
          "id": id,
          "name": key
        })
    print(json.dumps({
      "resp": resps
    }))
    return self

if __name__ == "__main__":

  raw_meta = sys.argv[1]
  meta = json.loads(raw_meta)

  methods = [
    {"name": "get", "step_args": {"target":meta["target"]}},
    *[
      {
        "name": "pipe", 
        "step_args": {
          "func_expr":f["function"], 
          "target": f["target"] if "target" in f else meta["target"], 
          "result": f["result"] if "result" in f else None,
          "kwargs": f["args"] if "args" in f else None,
          "type": f["type"] if "type" in f else "standard"
        }
      }
      for f in meta["steps"]
    ],
    {"name": "set", "step_args": {}}
  ]
  errFile = open(".dac/errFile.txt", "a+")
  try:
    chain = Chain(meta["name"], meta["id"])

    for m in methods:
      method = getattr(Chain, m["name"])
      method(chain, **m["step_args"])
  except Exception as e:
    errFile.write("%s\n" %datetime.now())
    errFile.write(''.join(tb.format_exception(None, e, e.__traceback__)))
    errFile.write("\n")
    
  errFile.close()