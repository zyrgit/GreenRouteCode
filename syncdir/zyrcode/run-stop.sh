#!/bin/bash

MYDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
echo $MYDIR
MyIP=$(sh $HOME/get_my_ip.sh)
echo $MyIP
if [ -z "$MyIP" ]; then
	echo "You need to modify get IP in shell script!"
	exit
fi

PORT=7784

PidFile=$MYDIR/logs/$MyIP.pid

netstat -lant | grep $PORT

if [ -f "$PidFile" ] ; then
    echo "rm -f $PidFile"
    rm -f $PidFile
else
	echo "Not running!"
fi

kill -9 $(lsof -t -i:$PORT)

echo "DONE: kill port $PORT"
