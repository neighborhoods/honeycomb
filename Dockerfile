FROM python:3
WORKDIR /usr/src/app

ARG NHDS_PYPI_USERNAME
ARG NHDS_PYPI_PASSWORD

COPY . .

RUN apt-get update; apt-get install -y build-essential curl unzip python-dev libsasl2-dev gcc
RUN pip install pipenv

RUN export USER_ENCODED=$(python a.py $NHDS_PYPI_USERNAME) && \
    export PASS_ENCODED=$(python a.py $NHDS_PYPI_PASSWORD) && \
    pip install --extra-index-url https://$USER_ENCODED:${PASS_ENCODED}@pypi.neighborhoods.com/simple --verbose river==1.1.1
RUN export USER_ENCODED=$(python a.py $NHDS_PYPI_USERNAME) && \
    export PASS_ENCODED=$(python a.py $NHDS_PYPI_PASSWORD) && \
    echo $USER_ENCODED && \
    pipenv install --dev --system --deploy
