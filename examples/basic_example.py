import pmemkv


def callback(key):
    mem_view = memoryview(key)
    print(mem_view.tobytes().decode())


print("Starting engine")
db = pmemkv.Database(r"vsmap", '{"path":"/dev/shm","size":1073741824}')
print("Put new key")
db.put("key1", "value1")
assert db.count_all() == 1

print("Reading key back")
assert db.get_string("key1") == "value1"

print("Iterating existing keys")
db.put("key2", "value2")
db.put("key3", "value3")
db.get_keys(lambda k: print(f"visited: {memoryview(k).tobytes().decode()}"))

print("Get single value")
db.get("key1", callback)

print("Get single value and key in lambda expression")
key = "key1"
db.get(
    key,
    lambda v, k=key: print(
        f"key: {k} with value: " f"{memoryview(v).tobytes().decode()}"
    ),
)

print("Removing existing key")
db.remove("key1")
assert not db.exists("key1")

print("Stopping engine")
db.stop()
