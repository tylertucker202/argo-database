#!/bin/bash

echo 'Start of rsync and List'
DATE=`date +%y-%m-%d-%H:%M`
echo $DATE
FTPDIR='/home/tylertucker/ifremer/'
ARGODIR='/home/tylertucker/argo-database/'
QUEUEDIR=$ARGODIR'queuedFiles/'
OUTPUTNAME=$QUEUEDIR'ALL-DACS-list-of-files-synced-'$DATE'.txt'
echo 'Starting rsync: writing to '$FTPDIR
#Sync only /profiles/[RDM]*
rsync -arvzhim --delete --include='**/'\
 --include='**/profiles/[RDMS]*.nc'\
 --exclude='*'\
 --exclude='**/profiles/B*'\
 vdmzrs.ifremer.fr::argo $FTPDIR > $OUTPUTNAME$
ENDDATE=`date +%y-%m-%d-%H:%M`
echo 'End of rsync and List'
echo $ENDDATE

echo 'Starting to add DB'
cd $ARGODIR
python3.6 process_queue.py kadavu
PYENDDATE=`date +%y-%m-%d-%H:%M`
echo 'Added new files to DB'
echo $PYENDDATE

