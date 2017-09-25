#!/bin/bash

echo 'Start of rsync and List'
DATE=`date +%y-%m-%d-%H:%M`
echo $DATE
FTPDIR='/home/tyler/Desktop/argo/argo-database/ifremer/'
#FTPDIR='/home/gstudent4/Desktop/ifremer/'
QUEUEDIR='/home/tyler/Desktop/argo/argo-database/queuedFiles/'
#QUEUEDIR='/home/gstudent4/Desktop/argo-database/queuedFiles/'
OUTPUTNAME=$QUEUEDIR'ALL-DACS-list-of-files-synced-'$DATE'.txt'
echo 'Starting rsync: writing to '$FTPDIR
rsync -arvzhim --delete --include='*/' --include='*_prof.nc' --exclude='*' vdmzrs.ifremer.fr::argo $FTPDIR > $OUTPUTNAME
ENDDATE=`date +%y-%m-%d-%H:%M`
echo 'End of rsync and List'
echo $ENDDATE
