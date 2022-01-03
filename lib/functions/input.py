import numpy as np
import pandas as pd

def ffill(data):
  mask = pd.isnull(data)
  idx = np.where(~mask,np.arange(mask.size),0)
  np.maximum.accumulate(idx, out=idx)
  return data[idx], None

def bfill(data):
  mask = pd.isnull(data)
  idx = np.where(~mask,np.arange(mask.size),0)
  np.minimum.accumulate(idx, out=idx)
  return data[idx], None

def cfill(data, val=None):
  if val == None:
    raise
  return np.where(pd.isnull(data),np.full(data.size, val),data), None

def meanfill(data):
  avg = np.nanmean(data)
  idx = np.where(np.isnan(data))
  data[idx[0]] = avg
  return data, None