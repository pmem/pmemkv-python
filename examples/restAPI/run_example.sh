#!/bin/bash

TEST_PATH=${TEST_PATH:-/dev/shm/pmemkvREST_pool}
APP_DIR=${APP_DIR:-$PWD}

assert() {
	echo "${1}"
	if [ "${1}" != "${2}" ]
	then
		echo ""Assertion failed:  \"$1\" not equal"\"$2\""
		exit 1
	fi
}

run_server() {
	TEST_PATH=$1 gunicorn --chdir ${APP_DIR} --daemon -b localhost:8000 pmemkvREST:app
	pid=$(ps aux |grep gunicorn |grep pmemkvREST | awk '{ print $2 }')
	until curl -s http://localhost:8000
	do
		sleep 0.1
	done
	echo ${pid}
}

shutdown_server() {
	pid=$1
	kill ${pid}
}

if [ ! -f ${TEST_PATH} ]; then
	echo "Creating pool for pmemkv"
	pmempool create -s 1073741824 obj -l pmemkv ${TEST_PATH}
fi
echo "Run server with pmemkv concurrent hash map as storage"
pid=$(run_server ${TEST_PATH})


echo "Put data into database"
curl -s -H "Content-type: application/json" -X PUT http://localhost:8000/db -d '{"message": "Hello Data"}'
echo "Read back data:"
assert "$(curl -s http://localhost:8000/db/message)" "\"Hello Data\""
echo "Shutdown server"
shutdown_server ${pid}
echo "Rerun server"
pid=$(run_server ${TEST_PATH})
echo "Read data written in previous session:"
assert "$(curl -s http://localhost:8000/db/message)" "\"Hello Data\""
echo "Remove data"
$(curl -s -X DELETE http://localhost:8000/db/message)
echo "Data was removed:"
assert "$(curl -s http://localhost:8000/db/message)" ""
echo "Shutdown server"
shutdown_server ${pid}
