FROM python:3
WORKDIR /usr/src/app

ARG NHDS_PYPI_USERNAME
ARG NHDS_PYPI_PASSWORD

COPY . .

RUN USER_ENCODED=$(python a.py $NHDS_PYPI_USERNAME) && \
    PASS_ENCODED=$(python a.py $NHDS_PYPI_PASSWORD) && \
    pip install --extra-index-url https://$USER_ENCODED:${PASS_ENCODED}@pypi.neighborhoods.com/simple --verbose river==1.1.1

RUN echo $NHDS_PYPI_USERNAME
RUN echo ${NHDS_PYPI_USERNAME}
RUN echo $USER_ENCODED
RUN echo ${USER_ENCODED}

RUN pip install pipenv
RUN pipenv install --dev --system --ignore-pipfile
