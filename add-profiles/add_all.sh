#!/bin/bash

echo "Starting add_all.sh"
DATE=`date +%y-%m-%d-%H:%M`
echo $DATE

HOSTNAME=`hostname`
echo 'Hostname is'
echo $HOSTNAME
case $HOSTNAME in
  (carby) 
		echo "this is carby"
		echo "adding minor dacs"
                /home/tyler/anaconda3/envs/argo/bin/python add_minor_dacs.py
		echo "adding coriolis"
		/home/tyler/anaconda3/envs/argo/bin/python add_coriolis.py
		echo "adding aoml"
		/home/tyler/anaconda3/envs/argo/bin/python add_aoml.py
		;;
  (kadavu.ucsd.edu) 
		echo "this is kadavu"
		echo "adding minor dacs"
                python3.6 add_minor_dacs.py
		echo "adding coriolis"
		python3.6 add_coriolis.py
		echo "adding aoml"
		python3.6 add_aoml.py
		;;
  (*) 
        	echo "this lab 416"
		echo "adding minor dacs"
                /home/gstudent4/anaconda2/envs/argo/bin/python add_minor_dacs.py
		echo "adding coriolis"
		/home/gstudent4/anaconda2/envs/argo/bin/python add_coriolis.py
		echo "adding aoml"
		/home/gstudent4/anaconda2/envs/argo/bin/python add_aoml.py
		;;
esac
