#!/bin/bash

echo 'Start of rsync and List'
DATE=`date +%y-%m-%d-%H:%M`
echo $DATE

HOSTNAME=`hostname`
echo 'Hostname is'
echo $HOSTNAME
case $HOSTNAME in
  (carby) 
		echo "this is carby"
                FTPDIR='/storage/ifremer/'
		ARGODIR='/home/tyler/Desktop/argo-database/'
		;;
  (kadavu.ucsd.edu) 
		echo "this is kadavu"
		FTPDIR='/home/tylertucker/ifremer/'
		ARGODIR='/home/tylertucker/Desktop/argo-database/'
		;;
		(*) 
        	echo "this lab 416"
		FTPDIR='/home/gstudent4/Desktop/ifremer/'
		ARGODIR='/home/gstudent4/Desktop/argo-database/'
		;;
esac

QUEUEDIR=$ARGODIR'queuedFiles/'
OUTPUTNAME=$QUEUEDIR'ALL-DACS-list-of-files-synced-'$DATE'.txt'

echo 'Starting rsync: writing to '$FTPDIR
#Sync only /profiles/[RDM]*

rsync -arvzhim --delete --include='**/' --include='**/profiles/[RDM]*.nc' --exclude='*' --exclude='**/profiles/B*' vdmzrs.ifremer.fr::argo $FTPDIR > $OUTPUTNAME
ENDDATE=`date +%y-%m-%d-%H:%M`
echo 'End of rsync and List'
echo $ENDDATE

echo 'Starting to add DB'
#PYTHONPATH='/home/gstudent4/anaconda2/envs/argo/bin/python'
cd $ARGODIR

case $HOSTNAME in
  (carby) 
                /home/tyler/anaconda3/envs/argo/bin/python processQueue.py
		;;
  (kadavu.ucsd.edu) 
		python3.6 processQueue.py
		;;
  (*)
		/home/gstudent4/anaconda2/envs/argo/bin/python processQueue.py
		;;
esac

PYENDDATE=`date +%y-%m-%d-%H:%M`
echo 'Added new files to DB'
echo $PYENDDATE
