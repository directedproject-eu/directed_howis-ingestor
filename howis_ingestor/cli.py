import os
import tempfile
from os.path import join
from ftplib import FTP

import click
from loguru import logger

from howis_ingestor import parser
from howis_ingestor.stager import Stager


default_stage_dir = join(tempfile.gettempdir(), "howis_staging")


@click.command()
@click.option("-u", "--username", 
              default=lambda: os.environ.get("HOWIS_FTP_USERNAME", ""),
              help="Username for the FTP connection. Alternatively set HOWIS_FTP_USERNAME.")
@click.option("-w",
              "password",
              help="Prompt for password (place the flag at the end of the command!). Alternatively set HOWIS_FTP_PASSWORD.",
              prompt=True, 
              prompt_required=False,
              hide_input=True)
@click.option("-s",
              "--stage-dir",
              "stage_dir",
              default=default_stage_dir,
              help="Directory containing CSA data to be ingested.")
@click.option("--dry-run",
              "dry_run",
              default=False,
              help="Connect and parse HOWIS data but skips CSA ingestion.")
@click.option("-e",
              "--encoding",
              default="ISO-8859-1",
              help="Encoding to use for reading remote files.")
@click.option("-d",
              "--destination",
              default="http://localhost:5000",
              help="Destination URL where to send CSA data to.")
@click.argument("ftp_url")
def main(username: str, password: str, stage_dir: str, dry_run: bool, encoding: str, ftp_url: str, destination: str):
    
    password = password if password else os.environ.get("HOWIS_FTP_PASSWORD", "")
    if not password:
        logger.error("HOWIS_FTP_PASSWORD is not set! Use -w flag for password prompt.")
        exit(1)
    
    ftp = None
    logger.info(f"Get latest data ..")
    
    try:
        logger.info(f"Establish connection with user '{username}' to '{ftp_url}'")
        ftp = FTP(ftp_url, encoding=encoding)
        ftp.login(user=username, passwd=password)
        ftp.dir()  # print remote dir content
        
        kontakt = parser.parse_kontakt(ftp)
        pegelstamm = parser.parse_pegelstamm(ftp)
        pegeldaten = parser.parse_pegeldaten(ftp)
        
        stager = Stager(staging_dir=stage_dir, csa_base_url=destination)
        staged_systems = stager.stage_systems(kontakt, pegelstamm)
        staged_datastreams = stager.stage_datastreams(pegelstamm, pegeldaten)
        staged_observations = stager.stage_observations(pegeldaten)
        
    except Exception as e:
        logger.error(f"Failed to ingest data: {e}")
        exit(-1)
    finally:
        if ftp is not None:
            logger.debug("Closing FTP connection.")
            ftp.close()
    
    logger.info("done!")


if __name__ == '__main__':  # pragma: no cover
    main()  # pylint: disable=no-value-for-parameter
