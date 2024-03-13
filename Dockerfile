# BSD 3-Clause All rights reserved.
#
# SPDX-License-Identifier: BSD 3-Clause

# This Dockerfile is used to build the iRODS-K8s-Supervisor image

FROM python:3.12.2-slim

# update the image base
RUN apt-get update && apt-get -y upgrade

# update pip
RUN pip install --upgrade pip

# clear the apt cache
RUN apt-get clean

# get some credit
LABEL maintainer="powen@renci.org"

# get the build argument that has the version
ARG APP_VERSION=$(APP_VERSION)

# now add the version arg value into a ENV param
ENV APP_VERSION=$APP_VERSION

# create a new non-root user and switch to it
RUN useradd --create-home -u 999 nru
USER nru

# Create the directory for the code and cd to it
WORKDIR /repo/iRODS-K8s-Supervisor

# Copy in just the requirements first for caching purposes
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

# Copy in the rest of the code
COPY . .

# start the service entry point
ENTRYPOINT ["python", "main.py"]
