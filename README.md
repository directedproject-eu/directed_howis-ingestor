# Overview

A simple HOWIS data ingestor.
Applies ingestions in tree steps:

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

## Usage

After installation, the ingestor can be run:

```sh
python howis_ingestor -u <user> <ftp_url> -w
```

Add the `--help` flag to get more information on available options.
For example, you can export your credentials to your shell environment for safety reasons:

```sh
export HOWIS_FTP_USERNAME=<user>
export HOWIS_FTP_PASSWORD=<password>
python howis_ingestor <ftp_url>
```

To let the ingestor prompt for a password use the `-w` flag.
