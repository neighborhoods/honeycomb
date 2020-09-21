FROM python:3
WORKDIR /usr/src/app

ARG NHDS_PYPI_USERNAME
ARG NHDS_PYPI_PASSWORD

COPY . .

RUN pip install --extra-index-url https://$NHDS_PYPI_USERNAME:${NHDS_PYPI_PASSWORD}@pypi.neighborhoods.com/simple --verbose river==1.1.1

RUN pip install pipenv

RUN export USER_ENCODED=$(python a.py $NHDS_PYPI_USERNAME) && \
    export PASS_ENCODED=$(python a.py $NHDS_PYPI_PASSWORD) && \

    # echo $USER_ENCODED && \
    # pipenv install --dev --system --deploy
