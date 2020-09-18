FROM python:3
WORKDIR /usr/src/app

ARG NHDS_PYPI_USERNAME
ARG NHDS_PYPI_PASSWORD

RUN pip install --extra-index-url https://${NHDS_PYPI_USERNAME}:${NHDS_PYPI_PASSWORD}@pypi.neighborhoods.com/simple --verbose river==1.1.1 

COPY . .

RUN pip install pipenv
RUN pipenv install --dev --system --ignore-pipfile
