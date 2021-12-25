from pygoridge import RPC, SocketRelay

def connect():
  rpc = RPC(SocketRelay("127.0.0.1", 42586))
  return rpc

# or, using factory
# tcp_relay = create_relay("tcp://127.0.0.1:42586")

# print(rpc("Store.Put", {"Key": "pytest", "Value": [1,2,3]}))
# print(rpc("Store.Get", {"Key": "pytest"}))
# rpc.close()     # close underlying socket connection

# or using as a context manager
# with RPC(tcp_relay) as rpc:
#     print(rpc("App.Hi", "Antony, again"))