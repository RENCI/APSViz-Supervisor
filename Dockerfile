# This Dockerfile is used to build the APSVIZ-Supervisor image
# starts with the python image
# installs nano
# creates a directory for the repo
# gets the APSVIZ-Supervisor repo
# and runs main which enables the supervisor

# leverage the renci python base image
FROM renciorg/renci-python-image:v0.0.1

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

# make sure everything is read/write in the repo code
RUN chmod 777 -R .

# install all required packages
RUN pip install -r requirements.txt

# debug only - copy in test supervisor code. other wise the repo code is fine
COPY ./src/job_supervisor.py /repo/APSVIZ-Supervisor/supervisor/src/job_supervisor.py
COPY ./src/job_create.py /repo/APSVIZ-Supervisor/supervisor/src/job_create.py

# add in the base config file
# ADD ./base_config.json /repo/APSVIZ-Supervisor/supervisor

# install requirements
RUN pip install -r requirements.txt

# switch to the non-root user (nru). defined in the base image
USER nru

# start the service entry point
ENTRYPOINT ["python", "main.py"]