#!/bin/bash
if ps -ef | grep -v grep | grep 'Main.py' ; then
	if ps -ef | grep -v grep | grep "sudo pkill -9 -f python3" ; then
		# Get a list of PIDs for processes named "my_process"
		pids=$(pgrep -f "sudo pkill -9 -f python3")

		# Kill all processes with the given PIDs
		for pid in $pids; do
		  sudo kill -9 $pid
		done
	fi		
	printf 'Program already running %s\n'
	exit 0
else 
	command="python3 /home/cmdadmin/Datalake\ Pusher/CurePulse/RemoveFiles.py"
	eval $command
	bash autorun.sh
fi
