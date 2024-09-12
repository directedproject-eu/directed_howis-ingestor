# Overview

A simple HOWIS data ingestor.

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

You can export your credentials to your shel environment for safety reasons:

```sh
export HOWIS_FTP_USERNAME=<user>
export HOWIS_FTP_PASSWORD=<password>
python howis_ingestor <ftp_url>
```

To let the ingestor prompt for a password use the `-w` flag.
There is also a `--help` to geht more information.
