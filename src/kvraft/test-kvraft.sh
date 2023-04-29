trap 'exit 1' INT
echo "Starting test..."
go test -c -race
chmod +x ./kvraft.test
DIR=output_$3
rm -rf $DIR
mkdir $DIR
mv ./kvraft.test $DIR
cd $DIR
echo "Running test $1 for $2 iterations..."
for i in $(seq 1 "$2"); do
    echo -ne "\r$i/$2 "
    LOG="$1_$i.log"

    if time ./kvraft.test -test.run "$1" -test.count=1 &> "$LOG"; then
        echo "Success"
        rm $LOG
    else
        echo "Failed - saving log at FAILED_$LOG"
        mv "$LOG" "FAILED_$LOG"
    fi
done
rm kvraft.test