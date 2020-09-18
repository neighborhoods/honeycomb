FROM python:3
WORKDIR /usr/src/app

ARG NHDS_PYPI_USERNAME
ARG NHDS_PYPI_PASSWORD

COPY . .
RUN python a.py
RUN pip install pipenv
RUN pipenv install --dev --system --ignore-pipfile
