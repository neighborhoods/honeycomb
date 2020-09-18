FROM python:3
WORKDIR /usr/src/app

ARG NHDS_PYPI_USERNAME
ARG NHDS_PYPI_PASSWORD

ARG test_username=$NHDS_PYPI_USERNAME

RUN echo $test_username

COPY . .
RUN pip install pipenv
RUN pipenv install --dev --system --ignore-pipfile
