FROM python:3
WORKDIR /usr/src/app

ARG NHDS_PYPI_USERNAME
ARG NHDS_PYPI_PASSWORD

COPY . .

RUN USER_ENCODED=$(python a.py $NHDS_PYPI_USERNAME)
RUN echo $USER_ENCODED
RUN PASS_ENCODED=$(python a.py $NHDS_PYPI_PASSWORD)
RUN pip install --extra-index-url https://$USER_ENCODED:${PASS_ENCODED}@pypi.neighborhoods.com/simple --verbose river==1.1.1

RUN pip install pipenv
RUN pipenv install --dev --system --ignore-pipfile
