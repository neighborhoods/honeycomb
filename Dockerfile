FROM python:3
WORKDIR /usr/src/app

ARG NHDS_PYPI_USERNAME
ARG NHDS_PYPI_PASSWORD

COPY . .

RUN apt-get update; apt-get install -y libsasl2-dev

RUN pip install pipenv

RUN export NHDS_PYPI_USERNAME=$(python a.py $NHDS_PYPI_USERNAME) && \
    export NHDS_PYPI_PASSWORD=$(python a.py $NHDS_PYPI_PASSWORD) && \
    pipenv install --dev --system --deploy
