FROM python:3.7-slim
WORKDIR /usr/src/app

ARG NHDS_PYPI_USERNAME
ARG NHDS_PYPI_PASSWORD

RUN apt-get update; apt-get install -y build-essential gcc git libsasl2-dev

RUN pip install pipenv

COPY . .

RUN pipenv install --dev --system --deploy
