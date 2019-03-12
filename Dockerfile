FROM continuumio/miniconda3

#RUN cd /usr/src/argo-database
WORKDIR /usr/src/argo-database

RUN groupadd -g 998 argouser && \
    useradd -r -u 998 -g argouser argouser
# copies requirements.txt from host dir to container work dir
COPY ./requirements.txt ./requirements.txt
#need to install netcdf libraries and cron
RUN apt-get update && \
    apt-get --assume-yes install libhdf5-serial-dev netcdf-bin libnetcdf-dev nano && \
    pip install -r requirements.txt
#CMD echo 'we ah doin da test' `date` > cron.log
USER argouser
CMD cd /usr/src/argo-database/test && /opt/conda/bin/python testTmpFunctions.py >> ./../cron.log 2>&1
#docker build -t argo-db:1.0
#docker run --net=host -v /home/tyler/Desktop/argo-database:/usr/src/argo-database/ argo-db:1.0