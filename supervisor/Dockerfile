# This Dockerfile is used to build the APSVIZ-Supervisor image
# starts with the python image
# installs nano
# creates a directory for the repo
# gets the APSVIZ-Supervisor repo
# and runs main which enables the supervisor

FROM python:3.9.7

# get some credit
LABEL maintainer="powen@renci.org"

# install basic tools
RUN apt-get update
RUN apt-get install -yq nano

# create log level env param (debug=10, info=20)
ENV LOG_LEVEL 20

# make a directory for the repo
RUN mkdir /repo

# go to the directory where we are going to upload the repo
WORKDIR /repo

# get the latest code
RUN git clone https://github.com/RENCI/APSVIZ-Supervisor.git

# go to the repo dir
WORKDIR /repo/APSVIZ-Supervisor

# add in the base config file
ADD ./base_config.json /repo/APSVIZ-Supervisor/supervisor

# install requirements
RUN pip install -r requirements.txt

# start the service entry point
ENTRYPOINT ["python", "main.py"]