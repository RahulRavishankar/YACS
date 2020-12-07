CS322:BIG DATA

Final Class Project Report

Project: Yet Another Centralized Scheduler                                Date: 01/12/2020

SNo.  Name.               SRN.              Class/Section
1.    Gaurika Poplai -    PES1201800374.    5 I
2.    Keerthana Mahadev - PES1201800768.    5 E
3.    Nikita J. Raj -     PES1201800808.    5 H
4.    Rahul KR -          PES1201802064.    5 J



Execution instructions: 

To start the workers, open up a terminal for each worker and run
python3 worker.py <port_number> <worker_id>

To start the master, run
python3 master.py config.json <algorithm_name>
(the algorithms arguments to be passed are RANDOM, RR, and LL)

To run the plotting file, to analyse the worker performance based on the logs written,
python3 analysis.py YACS_logs.log
