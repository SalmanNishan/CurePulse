#!/bin/bash
cd /home/cmdadmin/'Datalake Pusher'/CurePulse/auto_email/
python3 main.py
python3 daily_exception_calls.py

cd /home/cmdadmin/'Data Ambient Intelligence'/'CSV Database'/
python3 get_agents_names.py

cd /home/cmdadmin/'Datalake Pusher'/CurePulse/server_stats_email/
php dailyServerStats.php

