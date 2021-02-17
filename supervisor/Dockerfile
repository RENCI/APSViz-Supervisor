# This Dockerfile is used to build the aragorn ranker image
# starts with the python image
# installs nano
# creates a directory for the repo
# gets the aragorn-ranker repo
# and runs main which enables the gunicorn web server

FROM python:3.8.5

# get some credit
LABEL maintainer="powen@renci.org"

# install basic tools
RUN apt-get update
#RUN apt-get install -yq \
#    nano

# make a directory for the repo
RUN mkdir /repo

# go to the directory where we are going to upload the repo
WORKDIR /repo

# get the latest code
RUN git clone https://github.com/RENCI/APSVIZ-k8s.git

# go to the repo dir
WORKDIR /repo/APSVIZ-k8s

# add in the real config files
ADD ./supervisor_config.json /repo/APSVIZ-k8s/supervisor
ADD ./base_config.json /repo/APSVIZ-k8s/supervisor

# create a log details
RUN mkdir /data/log
ENV LOG_LEVEL 10
ENV LOG_DIR /data/log

# install requirements
RUN pip install -r requirements.txt

# start the service entry point
ENTRYPOINT ["bash"]