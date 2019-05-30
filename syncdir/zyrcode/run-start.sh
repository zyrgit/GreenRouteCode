#!/bin/bash

MYDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
echo $MYDIR
MyIP=$(sh $HOME/get_my_ip.sh)
echo $MyIP
if [ -z "$MyIP" ]; then
	echo "You need to modify get IP in shell script!"
	exit
fi

mkdir -p $MYDIR/logs/

PidFile=$MYDIR/logs/$MyIP.pid
echo $PidFile

if [ -f "$MYDIR/nohup.out" ] ; then
	rm -f $MYDIR/nohup.out
fi
if [ -f "$MYDIR/logs/$MyIP.err" ] ; then
	rm -f $MYDIR/logs/$MyIP.err
fi

if [ ! -f "$PidFile" ] ; then
	nohup python server.py & 
    echo "" >> $PidFile
else
	echo "WARNING: webserver is already running !"
fi



echo "DONE: nohup python server.py "
