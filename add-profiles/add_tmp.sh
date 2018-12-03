#!/bin/bash

echo 'Start of adding tmp'
ADDDIR=`pwd`
DATE=`date +%y-%m-%d-%H:%M`
echo $DATE
echo 'going to run add_from_tmp.py'
cd $ARGODIR
case $HOSTNAME in
  (carby)       
                /home/tyler/anaconda3/envs/argo/bin/python from_tmp.py
		;;
  (argovis) 
                cd $ADDDIR
                pwd
		 /root/anaconda3/envs/argo/bin/python from_tmp.py
		;;
  (*)
                cd $ADDDIR
                pwd
		/root/anaconda3/envs/argo/bin/python3.6 from_tmp.py
		;;
esac
PYENDDATE=`date +%y-%m-%d-%H:%M`
echo 'finished running add_from_tmp.py'
echo $PYENDDATE
