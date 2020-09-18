FROM python:3
WORKDIR /usr/src/app

ARG NHDS_PYPI_USERNAME
ARG NHDS_PYPI_PASSWORD

COPY . .

RUN pip install pipenv

ENV USER_ENCODED=$(python a.py $NHDS_PYPI_USERNAME)
ENV PASS_ENCODED=$(python a.py $NHDS_PYPI_PASSWORD)

RUN pipenv install --dev --system --ignore-pipfile
