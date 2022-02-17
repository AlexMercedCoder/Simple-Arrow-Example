#!/usr/bin/env sh

set -e

cd "$(dirname "$0")"

CLASS_PATH="./lib/flight-sql-7.0.0-SNAPSHOT-jar-with-dependencies.jar:./src"
javac -cp $CLASS_PATH src/BasicConnection.java
java -cp $CLASS_PATH BasicConnection "$@"
