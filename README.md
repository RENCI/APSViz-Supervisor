<!--
SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.
SPDX-FileCopyrightText: 2023 Renaissance Computing Institute. All rights reserved.

SPDX-License-Identifier: GPL-3.0-or-later
SPDX-License-Identifier: LicenseRef-RENCI
SPDX-License-Identifier: MIT
-->
![image not found](renci-logo.png "RENCI")

# APSViz Supervisor
A Workflow manager that creates and monitors K8s jobs for each data processing component.

#### Licenses...
[![MIT License](https://img.shields.io/badge/License-MIT-orange.svg)](https://github.com/RENCI/APSVIZ-Supervisor/tree/master/LICENSE)
[![GPLv3 License](https://img.shields.io/badge/License-GPL%20v3-yellow.svg)](https://opensource.org/licenses/)
[![RENCI License](https://img.shields.io/badge/License-RENCI-blue.svg)](https://www.renci.org/)
#### Components and versions...
[![Python](https://img.shields.io/badge/Python-3.11.6-orange)](https://github.com/python/cpython)
[![Linting Pylint](https://img.shields.io/badge/Pylint-%203.0.2-yellow)](https://github.com/PyCQA/pylint)
[![Pytest](https://img.shields.io/badge/Pytest-%207.4.3-blue)](https://github.com/pytest-dev/pytest)
#### Build status...
[![PyLint the codebase](https://github.com/irods-contrib/iRODS-K8s-Supervisor/actions/workflows/pylint.yml/badge.svg)](https://github.com/irods-contrib/iRODS-K8s-Supervisor/actions/workflows/pylint.yml)
[![Build and push the Docker image](https://github.com/irods-contrib/iRODS-K8s-Supervisor/actions/workflows/image-push.yml/badge.svg)](https://github.com/irods-contrib/iRODS-K8s-Supervisor/actions/workflows/image-push.yml)

## Description
This product is a workflow manager that sequences microservices deployed in a K8s environment.

The APSViz Job supervisor has the following features:
 - Integrates directly with K8s using the K8s API to create and monitor jobs.
 - Monitors each K8s job from creation to completion
 - Robust error handling.
 - Broadcasts the run state of each request (success, failure) using Slack.
 - K8s Job definitions for each workflow process are stored in a database.
 - K8s Job settings are configurable via the [iRODS-K8s settings application](https://github.com/RENCI/APSVIZ-Settings).

There are GitHub actions to maintain code quality in this repo:
 - Pylint (minimum score of 10/10 to pass),
 - Build/publish a Docker image.

Helm/k8s charts for this product are available at: [iRODS K8s Helm](https://github.com/irods/irods_k8s/tree/main/helm/irods-supervisor-settings).