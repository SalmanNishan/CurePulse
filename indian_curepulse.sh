#!/bin/bash
source LM_Finetuning/bin/activate
export CUDA_VISIBLE_DEVICES=0
if ps -ef | grep -v grep | grep 'Main.py' ; then
	printf 'Program already running %s\n'
	exit 0
else 
	command="python3 /home/cmdadmin/Datalake\ Pusher/CurePulse/RemoveFiles.py"
	eval $command
	cd /media/cmdadmin/Backup/Datalake\ Pusher/CurePulse/
	python3 Main.py
fi

