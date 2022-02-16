#!/bin/sh

postgres=$(docker run \
    -e ARCHIVE_READ_USER=ampel-readonly \
    -e ARCHIVE_READ_USER_PASSWORD=seekrit \
    -e ARCHIVE_WRITE_USER=ampel-writer \
    -e ARCHIVE_WRITE_USER_PASSWORD=seekrit \
    -e POSTGRES_DB=ztfarchive \
    -e POSTGRES_USER=ampel \
    -e POSTGRES_PASSWORD=seekrit \
    -v $(pwd)/tests/test-data/initdb/archive:/docker-entrypoint-initdb.d \
    -d \
    -P \
    --rm \
    ampelproject/postgres:14.1)

host_port() {
    docker inspect $1 | jq -r '.[0].NetworkSettings.Ports["'$2'"][0].HostPort'
}

POSTGRES_URI=postgresql://ampel:seekrit@localhost:$(host_port $postgres "5432/tcp")/ztfarchive

localstack=$(docker run \
    -e SERVICES=s3 \
    -e DEBUG=s3 \
    -d \
    -P \
    --rm \
    localstack/localstack:0.12.19.1)

LOCALSTACK_URI=http://localhost:$(host_port $localstack "4566/tcp")


cleanup() {
    echo stopping services...
    docker stop $postgres $localstack
}
trap cleanup SIGINT

echo postgres: $postgres
echo localstack: $localstack
echo POSTGRES_URI=\"$POSTGRES_URI\" LOCALSTACK_URI=\"$LOCALSTACK_URI\"

docker wait $postgres $localstack
