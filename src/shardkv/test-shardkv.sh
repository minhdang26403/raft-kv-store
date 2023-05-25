trap 'exit 1' INT
echo "Starting test..."
go test -c -race
chmod +x ./shardkv.test
DIR=output
rm -rf $DIR
mkdir $DIR
mv ./shardkv.test $DIR
cd $DIR
echo "Running test for $1 iterations..."
for i in $(seq 1 "$1"); do
    echo -ne "\r$i/$1 "
    LOG="$i.log"

    if time ./shardkv.test -test.count=1 &> "$LOG"; then
        echo "Success"
        rm $LOG
    else
        echo "Failed - saving log at FAILED_$LOG"
        mv "$LOG" "FAILED_$LOG"
    fi
done
rm shardkv.test