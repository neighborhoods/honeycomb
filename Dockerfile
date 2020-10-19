FROM python:3
WORKDIR /usr/src/app

ARG NHDS_PYPI_USERNAME
ARG NHDS_PYPI_PASSWORD

RUN apt-get update; apt-get install -y libsasl2-dev

RUN pip install pipenv
RUN pip freeze

COPY . .

RUN pipenv install --dev --system --deploy
