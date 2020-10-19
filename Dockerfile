FROM python:3.7-slim
WORKDIR /usr/src/app

ARG NHDS_PYPI_USERNAME
ARG NHDS_PYPI_PASSWORD

COPY . .

RUN apt-get update; apt-get install -y libsasl2-dev

RUN pip install pipenv

RUN pipenv install --dev --system --deploy
