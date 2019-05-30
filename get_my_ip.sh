#!/bin/bash

exec 2>/dev/null
# greengps
MyIP=`ifconfig eth0| grep -Eo 'inet (addr:)?([0-9]*\.){3}[0-9]*' | grep -Eo '([0-9]*\.){3}[0-9]*' | grep -v '127.0.0.1' | tr -d ' \t\n\r'`

if [ -z "$MyIP" ]; then 
	# Mac
	MyIP=`ifconfig en0| grep -Eo 'inet (addr:)?([0-9]*\.){3}[0-9]*' | grep -Eo '([0-9]*\.){3}[0-9]*' | grep -v '127.0.0.1' | tr -d ' \t\n\r'`
fi

if [ -z "$MyIP" ]; then
	# linux
	MyIP=`ifconfig eno1| grep -Eo 'inet (addr:)?([0-9]*\.){3}[0-9]*' | grep -Eo '([0-9]*\.){3}[0-9]*' | grep -v '127.0.0.1' | tr -d ' \t\n\r'`
fi

if [ -z "$MyIP" ]; then
	# thinkpad
	MyIP=`ifconfig enp0s25| grep -Eo 'inet (addr:)?([0-9]*\.){3}[0-9]*' | grep -Eo '([0-9]*\.){3}[0-9]*' | grep -v '127.0.0.1' | tr -d ' \t\n\r'`
fi

echo $MyIP
