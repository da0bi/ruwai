#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script for Ruwai datalogger service runs.
All networks with Ruwai dataloggers need to be present in the dictionaries in the 'INPUT PARAMETERS' section.
Script
...initiate a log file.
...establishes a ssh connection with the Ruwai datalogger.
...retrieves the Ruwai S/N.
...creates the directory structure on the local machine with options for before and after sd card exchange.
...downloads all the Ruwai log-files from /var/log/.
...checks if Ruwai datalogger is logging.
...checks status of sd card and gps.
...downloads logged data with following options
    1. download current mseed_tmp directory.
    2. download all logged data from the sd card.
    3. download all logged data and clear sd card.

Required python packages:   logging
                            pathlib
                            datetime
                            time
                            subprocess
                            SSHLibrary

@ Daniel Binder
Bad Gastein, 18.04.2022

"""

import logging
import pathlib
from datetime import datetime
from time import sleep
import subprocess

from SSHLibrary import SSHLibrary


#################################
# INPUT PARAMETERS
#################################
# Dictionary for each network. Contains station names and S/N of assigned Ruwai datalogger.
SBK_stations = {
    "00006": "OBS",
    "0000B": "PIL",
    "00009": "MOR",
    "00008": "MIT",
    "00004": "STO",
}

KITZ_stations = {
    "00007": "BH1",
    "0000A": "BH2",
    "00005": "BH3",
}

NOW_stations = {
    "0000F": "NUKL",
    "0000G": "ZACP",
    "0000H": "ZACR",
    "0000K": "PATW",
    "0000J": "PATE",
    "0000I": "PATT",
}

# Dictionary of all the installed Ruwai networks.
networks = {
    "SBK_stations": SBK_stations,
    "KITZ_stations": KITZ_stations,
    "NOW_stations": NOW_stations,
}

# Credentials for ssh connection.
ip = "192.168.20.100"
user = "ruwai"
pwd = "pfauenauge"


#################################
# INITIATE LOG FILE
#################################
def get_logger_stream_handler(log_level="WARNING"):
    """Create a logging stream handler.

    Returns
    -------
    ch: logging.StreamHandler
        The logging filehandler.
    """
    ch = logging.StreamHandler()
    ch.setLevel(log_level)
    # formatter = logging.Formatter("#LOG# - %(asctime)s - %(process)d - %(levelname)s - %(name)s: %(message)s")
    formatter = logging.Formatter("%(message)s")
    ch.setFormatter(formatter)
    return ch


def get_logger_file_handler(filename, log_level="WARNING"):
    ch = logging.FileHandler(filename)
    ch.setLevel(log_level)
    # formatter = logging.Formatter("#LOG# - %(asctime)s - %(process)d - %(levelname)s - %(name)s: %(message)s")
    formatter = logging.Formatter("%(message)s")
    ch.setFormatter(formatter)
    return ch


logger_name = "ruwai_check"
logger = logging.getLogger(logger_name)
# Set the common log level to debug. The log level is refined for each handler.
logger.setLevel("DEBUG")

# Create a handler logging to stdout.
handler = get_logger_stream_handler(log_level="INFO")
logger.addHandler(handler)

# Create a handler logging to a file.
handler = get_logger_file_handler(filename="./ruwai_check.log", log_level="INFO")
logger.addHandler(handler)


#################################
# WELCOME
#################################
logger.info(
    """
************************************
Welcome to the Ruwai service script.
************************************
"""
)


#################################
# SSH CONNECTION
#################################
# Remove authentication key from another Ruwai datalogger.
home = str(pathlib.Path.home())
command = 'ssh-keygen -f "' + home + '/.ssh/known_hosts" -R ' + ip
subprocess.call(command, shell=True)

# Establish a ssh connection with the Ruwai datalogger.
ssh = SSHLibrary()
ssh.open_connection(ip)
ssh.login(user, pwd)

logger.info("\n...ssh-connection with Ruwai datalogger established.")


#################################
# CREATE LOCAL DIRECTORIES & DOWNLOAD RUWAI LOGS
#################################
# Get Ruwai S/N.
sn = ssh.execute_command("cat /etc/ruwai_serial")
# Initiate network and station in case there is no match
network = "unknown_network"
station = "unknown_station"
# Look up network and station assigned to the Ruwai S/N.
for n in networks:
    if sn in networks[n]:
        network = n
        station = networks[n][sn]

if station == "unknown_station":
    logger.info("\n...the Ruwai S/N does not fit to any station in the dictionaries.")
else:
    logger.info("\n...you are at the " + str(station) + " station (" + network + ").")

# Produce directory structure on local machine.
# First prompt user if the service is before or after the sd card swap.
option = input(
"""\n--------------------------------------------------------------------------
Is this the service BEFORE or AFTER the sd card exchange?\n
\t1. BEFORE the sd card exchange.
\t2. AFTER the sd card exchange.
\nChoose 1, or 2: """
)
print("--------------------------------------------------------------------------")

# Create service run and station directories
sr_dir = "sr" + str(datetime.today().strftime("%Y%m%d"))
st_dir = str(station) + "_sn_" + str(sn)
if option == '1':
    st_dir = st_dir + "_1_BEX"
    logger.info("\n--------------------------------------------------------------------------")
    logger.info("This is the service BEFORE the sd card exchange.")
    logger.info("--------------------------------------------------------------------------")
elif option == '2':
    st_dir = st_dir + "_2_AEX"
    logger.info("\n--------------------------------------------------------------------------")
    logger.info("This is the service AFTER the sd card exchange.")
    logger.info("--------------------------------------------------------------------------")

# Create full path of directories
path = pathlib.Path(
    str(pathlib.Path().absolute()),
    str(datetime.today().year),
    network,
    sr_dir,
    st_dir,
)
path.mkdir(parents=True, exist_ok=True)
# Get absolute path.
local_dest = str(pathlib.Path(path).absolute())

# Create directory path for log-files
path = pathlib.Path(local_dest, "var_log")
path.mkdir(parents=True, exist_ok=True)
log_dest = str(pathlib.Path(path).absolute())

logger.info("\n...directory structure on local machine done.")

# Download all Ruwai log files from /var/log/.
all_logs = ssh.list_files_in_directory("/var/log/")
logs = [f for f in all_logs if f.split(".")[0] == "ruwai"]
for f in logs:
    ssh.get_file("/var/log/" + f, log_dest + "/" + f)

logger.info("\n...all Ruwai log-files downloaded to " + log_dest + "/.")


#################################
# STATUS CHECKS
#################################
logger.info("\n\n--------------------------------------------------------------------------")
logger.info("Ruwai status checks initiated.")
logger.info("--------------------------------------------------------------------------")

# Get ruwaicom process id
pid = ssh.execute_command("pidof ruwaicom")
logger.info("Process ID of running ruwaicom software: " + str(pid))

# Check through size increase of mseed_tmp folder if datalogger is logging.
logger.info("\n\n--------------------------------------------------------------------------")
logger.info("Checking the current size of the mseed_tmp/ directory.")
logger.info("--------------------------------------------------------------------------")
s1 = int(ssh.execute_command("du -s /home/ruwai/ruwaicom/mseed_tmp | cut -f1"))
logger.info("The current size of the mseed_tmp/ directory is " + str(s1) + " kB.")
sleep(10)
s2 = int(ssh.execute_command("du -s /home/ruwai/ruwaicom/mseed_tmp | cut -f1"))
logger.info("The current size of the mseed_tmp/ directory is " + str(s2) + " kB.")
if s1 < s2:
    logger.info("\n--------------------------------------------------------------------------")
    logger.info("Ruwai IS logging :) ")
    logger.info("--------------------------------------------------------------------------")
else:
    logger.info("\n--------------------------------------------------------------------------")
    logger.info("Ruwai is NOT logging :(")
    logger.info("--------------------------------------------------------------------------")

# Check sd card status
logger.info("\n\n--------------------------------------------------------------------------")
logger.info("SD card status:")
logger.info("--------------------------------------------------------------------------")
f = log_dest + "/ruwai.log*"
s = "ruwaicom\[" + str(pid) + "\].*SD"
command = "cat " + f + ' | grep -a "' + s + '" | tail -n 10'
# Way to capture the subprocess output in the log-file
p1 = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
for line in p1.stdout:
    l = line.decode(encoding="utf-8", errors="ignore")
    logger.info(l)

# Check gps status
logger.info("\n\n--------------------------------------------------------------------------")
logger.info("GPS status:")
logger.info("--------------------------------------------------------------------------")
s = "ruwaicom\[" + str(pid) + "\].*GPS_FIX"
command = "cat " + f + ' | grep -a "' + s + '" | tail -n 10'
# Way to capture the subprocess output in the log-file
p1 = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
for line in p1.stdout:
    l = line.decode(encoding="utf-8", errors="ignore")
    logger.info(l)

# Check utc status
logger.info("\n\n--------------------------------------------------------------------------")
logger.info("UTC status:")
logger.info("--------------------------------------------------------------------------")
s = ".*UTC_AVAILABLE"
#s = "ruwaicom\[" + str(pid) + "\].*UTC_AVAILABLE"
command = "cat " + f + ' | grep -a "' + s + '" | tail -n 10'
# Way to capture the subprocess output in the log-file
p1 = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
for line in p1.stdout:
    l = line.decode(encoding="utf-8", errors="ignore")
    logger.info(l)

# Check last log file output
logger.info("\n\n--------------------------------------------------------------------------")
logger.info("Last log-file output:")
logger.info("--------------------------------------------------------------------------")
s = "ruwaicom\[" + str(pid) + "\]"
command = "cat " + f + ' | grep -a "' + s + '" | tail -n 10'
# Way to capture the subprocess output in the log-file
p1 = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
for line in p1.stdout:
    l = line.decode(encoding="utf-8", errors="ignore")
    logger.info(l)

# Print out Ruwai datafile system
logger.info("\n\n--------------------------------------------------------------------------")
logger.info("Ruwai datalogger filesystem output:")
logger.info("--------------------------------------------------------------------------")
output = ssh.execute_command("df -h")
logger.info(output)

# List all directories in /media/sd/mseed/.
logger.info("\n\n--------------------------------------------------------------------------")
logger.info(
    "Date of today: \t"
    + datetime.today().strftime("%Y-%m-%d")
    + "\nDay of year: \t"
    + datetime.today().strftime("%j")
)
logger.info("\nCurrent directories in /media/sd/mseed/" + str(datetime.today().year) + "/:")
logger.info("--------------------------------------------------------------------------")
output = ssh.execute_command("ls /media/sd/mseed/" + str(datetime.today().year))
logger.info(output)


#################################
# DATA DOWNLOAD
#################################
logger.info(
    """\n
--------------------------------------------------------------------------
Ruwai data download
--------------------------------------------------------------------------"""
)

# Prompt for download options.
option = input(
    """Download options:\n
\t0. Download the current mseed_tmp directory.
\t1. Download the current mseed_tmp directory.
\t2. Download all the log and mseed directories of the sd-card.
\t3. Download all the log and mseed directories and clear the sd-card.\n
Choose 0, 1, 2, or 3: """
)
if option == "0":
     logger.info("You chose download option 0:\nContinue without any data download.\n\n")
     pass
elif option == "1":
    ssh.get_directory("/home/ruwai/ruwaicom/mseed_tmp", local_dest)
    logger.info("You chose download option 1:\nCurrent mseed_tmp/ directory was downloaded to\n\n" + local_dest)
elif option == "2":
    ssh.get_directory("/media/.", local_dest, recursive=True)
    logger.info("You chose download option 2:\nAll the log and mseed directories were downloaded to\n\n" + local_dest)
elif option == "3":
    ssh.get_directory("/media/.", local_dest, recursive=True)
    ssh.execute_command("rm -r /media/sd/log")
    ssh.execute_command("rm -r /media/sd/mseed")
    logger.info("You chose download option 3:\nAll the log and mseed directories were downloaded to\n\n" + local_dest + "\n\nand the sd card was cleared.")


#################################
# FINISH UP
#################################
logger.info(
    """\n
************************************
All tasks finished.        Bye, Bye!
************************************
\n"""
)

# Move log-file to station folder.
command = "mv ./ruwai_check.log " + local_dest
subprocess.call(command, shell=True)
