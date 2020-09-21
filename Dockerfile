FROM python:3
WORKDIR /usr/src/app

ARG NHDS_PYPI_USERNAME
ARG NHDS_PYPI_PASSWORD

COPY . .

RUN pip install pipenv

RUN export USER_ENCODED=$(python a.py $NHDS_PYPI_USERNAME) && \
    export PASS_ENCODED=$(python a.py $NHDS_PYPI_PASSWORD) && \
    pip install --extra-index-url https://$USER_ENCODED:${PASS_ENCODED}@pypi.neighborhoods.com/simple --verbose river==1.1.1
    echo $USER_ENCODED && \
    pipenv install --dev --system --deploy
