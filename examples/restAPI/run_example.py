import pexpect
import os
import requests

class RestApiRunner:
    def __init__(self, pool_path):
        self.server = None
        self.pool_path = pool_path
        self.script_dir = os.path.dirname(os.path.realpath(__file__))

    def run(self):
        self.server = pexpect.spawn("/usr/bin/python3 pmemkvREST.py", env={"PMEMKV_POOL_PATH" : self.pool_path}, cwd=self.script_dir)
        self.server.expect("Starting server")

    def stop(self):
        self.server.close()

def create_pool(pool_path, pool_size):
    cmd = f"pmempool create -s {pool_size} -l pmemkv obj {pool_path}"
    if not os.path.exists(pool_path):
        out, exit_status = pexpect.run(cmd, withexitstatus=True)
        if exit_status !=  0:
           raise Exception(f"Cannot create pool: {out}")


if __name__ == "__main__":
    pool_path = os.environ.get("TEST_PATH") or "/dev/shm/pmemkvREST_pool"
    pool_size = os.environ.get("POOL_SIZE") or "1073741824"

    create_pool(pool_path, pool_size)
    server = RestApiRunner(pool_path)
    print("Run server with pmemkv concurrent hash map as storage")
    server.run()
    print("Put data into database")
    requests.put("http://localhost:8000/db", json={"message": "Hello Data"})
    print("Get list of all elements")
    all_keys = requests.get("http://localhost:8000/db")
    print(f"{'': <8}{all_keys}, {all_keys.content}")
    print("Read back data:")
    response1 = requests.get("http://localhost:8000/db/message")
    print(f"{'': <8}{response1}, {response1.content}")
    print("Shutdown server")
    server.stop()
    print("Rerun server")
    server.run()
    print("Read data written in previous session:")
    response2 = requests.get("http://localhost:8000/db/message")
    print(f"{'': <8}{response2}, {response2.content}")
    print("Remove data")
    response2 = requests.delete("http://localhost:8000/db/message")
    print("Data was removed:")
    response3 = requests.get("http://localhost:8000/db/message")
    print(f"{'': <8}{response3}, {response3.content}")
    print("Shutdown server")
    server.stop()
