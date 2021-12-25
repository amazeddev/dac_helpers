#!/usr/bin/python3
import numpy as np

def normalize(data):
  norm = np.linalg.norm(data)
  result = np.array(data)/norm
  return result, None

