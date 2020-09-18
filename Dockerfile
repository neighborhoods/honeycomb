FROM python:3
WORKDIR /usr/src/app

ARG NHDS_PYPI_USERNAME
ARG NHDS_PYPI_PASSWORD

RUN pip install -f https://${NHDS_PYPI_USERNAME}:${NHDS_PYPI_PASSWORD}@pypi.neighborhoods.com/simple river==1.1.1

COPY . .

RUN pip install pipenv
RUN pipenv install --dev --system --ignore-pipfile
