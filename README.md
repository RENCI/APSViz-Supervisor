<!--
BSD 3-Clause All rights reserved.

SPDX-License-Identifier: BSD 3-Clause
-->

[![iRODS](iRODS-Logo.png)](https:/irods.org)

# iRODS-K8s Supervisor
A Workflow manager that creates and monitors K8s jobs for each iRODS Test Request component.

#### License.
[![BSD License](https://img.shields.io/badge/License-BSD-orange.svg)](https://github.com/irods-contrib/iRODS-K8s-Supervisor/blob/master/LICENSE)

#### Components and versions.
[![Python](https://img.shields.io/badge/Python-3.12.6-orange)](https://github.com/python/cpython)
[![Linting Pylint](https://img.shields.io/badge/Pylint-%203.3.0-yellow)](https://github.com/PyCQA/pylint)
[![Pytest](https://img.shields.io/badge/Pytest-%208.3.3-blue)](https://github.com/pytest-dev/pytest)
[![Kubernetes API](https://img.shields.io/badge/Kubernetes%20API-%20v28.1.0-red)](https://kubernetes.io/docs/concepts/overview/kubernetes-api/)

#### Build status.
[![PyLint the codebase](https://github.com/irods-contrib/iRODS-K8s-Supervisor/actions/workflows/pylint.yml/badge.svg)](https://github.com/irods-contrib/iRODS-K8s-Supervisor/actions/workflows/pylint.yml)
[![Build and push the Docker image](https://github.com/irods-contrib/iRODS-K8s-Supervisor/actions/workflows/image-push.yml/badge.svg)](https://github.com/irods-contrib/iRODS-K8s-Supervisor/actions/workflows/image-push.yml)

## Description
This product is a workflow manager that sequences microservices deployed in a K8s environment.

The iRODS-K8s Job supervisor has the following features:
 - Integrates directly with K8s using the K8s API to create and monitor jobs.
 - Monitors each K8s job from creation to completion.
 - Robust error handling.
 - K8s Job definitions for each workflow process are stored in a database.
 - K8s Job settings are configurable via the [iRODS-K8s settings application](https://github.com/irods-contrib/iRODS-K8s-Settings).

There are GitHub actions to maintain code quality in this repo:
 - Pylint (minimum score of 10/10 to pass),
 - Build/publish a Docker image.

### How to build the Docker image for this product.

The Docker image must be placed in a container image registry and referenced in this component's deployment scripts.

```shell
docker build --build-arg APP_VERSION=<version> -f Dockerfile -t irods-k8s-supervisor:latest .
```

### K8s/Helm deployment charts for this product are available *[here](https://github.com/irods/irods_k8s/tree/main/helm/irods-supervisor)*.