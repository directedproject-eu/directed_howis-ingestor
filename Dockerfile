FROM python:3.12-alpine

ENV STAGE_DIR=./data
ENV HOWIS_FTP_USERNAME=
ENV HOWIS_FTP_PASSWORD=

ENV HOWIS_CSA_OVERRIDE=false
ENV HOWIS_CSA_DESINATION=
ENV HOWIS_CSA_USERNAME=
ENV HOWIS_CSA_PASSWORD=

RUN apk add --no-cache \
    git \
    gcc \
    clang \
    python3-dev \
    zlib-dev \
    proj-dev \
    proj-util \
    poetry

WORKDIR /app

# install dependencies
COPY pyproject.toml poetry.* ./ 
RUN poetry install

# copy sources
COPY . ./
# install scripts
RUN poetry install

ENTRYPOINT [ "poetry", "run", "ingestor" ]
