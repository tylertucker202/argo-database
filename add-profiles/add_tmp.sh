#!/bin/bash

echo 'Start of adding tmp'
DATE=`date +%y-%m-%d-%H:%M`
echo $DATE
echo 'going to run add_from_tmp.py'
cd $ARGODIR
case $HOSTNAME in
  (carby)       
                /home/tyler/anaconda3/envs/argo/bin/python from_tmp.py
		;;
  (argovis) 
		       /root/anaconda3/envs/argo/bin/python from_tmp.py
		;;
  (*)
		python add_from_tmp.py
		;;
esac
PYENDDATE=`date +%y-%m-%d-%H:%M`
echo 'finished running add_from_tmp.py'
echo $PYENDDATE
