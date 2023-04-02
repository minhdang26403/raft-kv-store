#!/usr/bin/env bash

trap 'exit 1' INT

export VERBOSE=1

echo "Compiling raft.go..."
go test -c -race
chmod +x ./raft.test
rm -rf output
mkdir output
echo "Running test $1 for $2 iterations..."
for i in $(seq 1 "$2"); do
    echo -ne "\r$i/$2 "
    LOG="$1_$i.log"

    if time ./raft.test -test.run "$1" -test.count=1 &> "output/$LOG"; then
        echo "Success"
        rm output/$LOG
    else
        echo "Failed - saving log at FAILED_$LOG"
        mv "output/$LOG" "output/FAILED_$LOG"
    fi
done
rm raft.test