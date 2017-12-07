#!/bin/bash

echo 'Start of rsync and List'
DATE=`date +%y-%m-%d-%H:%M`
echo $DATE
#FTPDIR='/storage/ifremer/'
#FTPDIR='/home/gstudent4/Desktop/ifremer/'
FTPDIR='/home/tylertucker/ifremer/'
#ARGODIR='/home/gstudent4/Desktop/argo-database/'
ARGODIR='/home/tylertucker/argo-database/'
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
#/home/gstudent4/anaconda2/envs/argo/bin/python processQueue.py ciLab
python3.6 processQueue.py kadavu
PYENDDATE=`date +%y-%m-%d-%H:%M`
echo 'Added new files to DB'
echo $PYENDDATE
