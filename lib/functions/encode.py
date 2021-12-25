import pandas as pd
import numpy as np

def onehot(data):
  """Encode categorical features as a one-hot numeric array."""

  b = pd.get_dummies(data)
  arr = np.array(b)

  results = [arr[:,c] for c in range(arr.shape[1])]
  return results, b.columns.tolist()

def digitize(data, bins=None, right=False):
  """Digitize data (set feature values to 0 or 1) according to a threshold."""
  
  if not bins:
    raise
  bins = np.array(bins)
  digitized = np.digitize(data, bins, right)
  d = pd.get_dummies(digitized)
  for i in range(len(bins) + 1):
    if i not in d.columns.tolist():
      d[i] = 0
  d = d[list(range(len(bins) + 1))]
  arr = np.array(d)
  
  results = [arr[:,c] for c in range(arr.shape[1])]
  return results, d.columns.tolist()
