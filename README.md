# 🚧 ARCHIVED 🚧

This repository was archived on 20.01.2026 and receives no maintenance.

## Overview

A simple data ingestor reading LHP data for OGC API for Connected Systems.
At this time, ingestions is optimized for HOWIS (Erftverband) data.

Applies ingestion in tree steps:

1. Downloading and parsing from FTP
1. Transform and stage CSA requests
1. Ingest CSA requests

> :bulb: **Note:**
>
> this tool is currently under developement and therefore work in progress.

## Setup

### Requirements

-   Python 3.11+

### Installation

```sh
$ poetry install
```

## Staging

After installation, the ingestor can be run:

```sh
python howis_ingestor -u <user> <ftp_url> -w
```

The command downloads and parses latest data from given FTP source, and finally prepares the output for ingestion into an OGC API for Connected Systems.

Add the `--help` flag to get more information on available options.
For example, you can export your credentials to your shell environment for safety reasons:

```sh
export HOWIS_FTP_USERNAME=<user>
export HOWIS_FTP_PASSWORD=<password>
python howis_ingestor <ftp_url>
```

To let the ingestor prompt for a password use the `-w` flag.

## Ingestion

Set the `-d` parameter to provide a destination URL for destination.
Add `HOWIS_CSA_USERNAME` and `HOWIS_CSA_PASSWORD` if the OGC API for Connected Systems requires basic authentication.

## Docker

Build and run ingestor via

```sh
docker build . -t directed/howis-ingestor:latest
docker run -e HOWIS_CSA_USERNAME=csa -e HOWIS_CSA_PASSWORD=csa -e HOWIS_FTP_USERNAME=<username> -e HOWIS_FTP_PASSWORD=<password> directed/howis-ingestor:latest --dry-run -d http://<csa_host> <ftp-url>
```

Optionally add `--network host` to ingest into a CSA instance running on your host instance.
