#!/bin/bash

echo 'Start of adding tmp'
DATE=`date +%y-%m-%d-%H:%M`
echo $DATE
PYTHONPATH='/root/anaconda3/envs/argo/bin/python'
echo 'going to run add_from_tmp.py'
cd $ARGODIR
case $HOSTNAME in
  (carby)       
                cd /home/tyler/Desktop/argo-database
                /home/tyler/anaconda3/envs/argo/bin/python add_from_tmp.py
		;;
  (argovis) 
                cd /home/tyler/argo-database
		/root/anaconda3/envs/argo/bin/python add_from_tmp.py
		;;
  (*)
		python add_from_tmp.py
		;;
esac
PYENDDATE=`date +%y-%m-%d-%H:%M`
echo 'finished running add_from_tmp.py'
echo $PYENDDATE
