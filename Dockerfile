FROM continuumio/miniconda3

WORKDIR /usr/src/argo-database
# copies requirements.txt from host dir to container work dir
COPY ./requirements.txt ./requirements.txt
#need to install netcdf libraries and cron
RUN apt-get update && \
    apt-get --assume-yes install libhdf5-serial-dev netcdf-bin libnetcdf-dev nano && \
    pip install -r requirements.txt
CMD cd /usr/src/argo-database/add-profiles && \
       python add_profiles.py --dbName argo --subset tmp --logName tmp.log --npes 1
