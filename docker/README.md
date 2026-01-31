\# Docker Deployment



\## Overview



This project includes a Docker-based setup using `docker-compose.yml` to run PostgreSQL and the ETL pipeline.



\## Prerequisites



\- Docker

\- Docker Compose



\## Services



\- \*\*postgres\*\*: PostgreSQL database with `staging` and `warehouse` schemas.

\- \*\*pipeline\*\*: Python container with all required dependencies installed.



\## Setup



1\. Copy `.env.example` to `.env` and update any credentials if needed.

2\. Build and start the services:



&nbsp;  ```bash

&nbsp;  docker-compose up --build



