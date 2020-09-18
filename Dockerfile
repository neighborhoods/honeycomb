FROM python:3
WORKDIR /usr/src/app

ARG NHDS_PYPI_USERNAME
ARG NHDS_PYPI_PASSWORD

RUN echo $NHDS_PYPI_USERNAME

COPY . .
RUN pip install pipenv
RUN pipenv install --dev --system --ignore-pipfile
