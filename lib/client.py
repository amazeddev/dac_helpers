from pygoridge import create_relay, RPC, SocketRelay

rpc = RPC(SocketRelay("127.0.0.1", 42586))

# or, using factory
tcp_relay = create_relay("tcp://127.0.0.1:42586")

# print(rpc("Store.Put", {"Key": "pytest", "Value": [1,2,3]}))
col1 = rpc("KVStore.List", {"Base": False})
print(col1)
# print(type(col1))
rpc.close()     # close underlying socket connection

# or using as a context manager
# with RPC(tcp_relay) as rpc:
#     print(rpc("App.Hi", "Antony, again"))