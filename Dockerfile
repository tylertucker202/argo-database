FROM continuumio/miniconda3
WORKDIR /usr/src/argo-database

# copies everything from host dir to container work dir
COPY . .
#need to install netcdf libraries
RUN apt-get update && apt-get --assume-yes install libhdf5-serial-dev netcdf-bin libnetcdf-dev 
# install python libraries
RUN pip install -r requirements.txt
#--net=host seems to work. use network_mode: "host" in docker-compose.

# run test scripts
WORKDIR /usr/src/argo-database/test
CMD [ "python", "./unitTest.py" ]