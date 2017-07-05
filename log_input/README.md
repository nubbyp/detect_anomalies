README

The solution is coded in Python. I use only very standard Python libraries including numpy, sets, datetime and json so no special installs needed.

All the code is done in one source file named detect_anomalies.py.

There is a single User class that contains the user_id, list of friends and list of purchases made. A dictionary of users is generated when the batch_input file is first loaded into the system. After that as data is streamed in from the logs, a temporary network is generated in memory for the user’s network and then latest network purchases. 