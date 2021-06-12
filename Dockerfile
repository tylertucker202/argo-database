FROM continuumio/miniconda3

WORKDIR /usr/src/argo-database

# Make RUN commands use 'bash --login':
SHELL ["/bin/bash", "--login", "-c"]

# copies conda environment from host dir to container work dir
COPY ./argo-conda-env.txt ./argo-conda-env.txt
#need to install netcdf libraries and nano
RUN apt-get update && \
    apt-get --assume-yes install libhdf5-serial-dev netcdf-bin libnetcdf-dev nano
RUN conda create --name argo --file argo-conda-env.txt
RUN conda activate argo
RUN pip install wget==3.2
CMD cd /usr/src/argo-database/add-profiles && \
       python add_profiles.py --dbName argo --subset tmp --logName tmp.log --npes 1
