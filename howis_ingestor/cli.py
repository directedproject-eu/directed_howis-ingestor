import os
from ftplib import FTP

import click
from loguru import logger

from howis_ingestor import utils, parser, csa_staging


@click.command()
@click.option("-u", "--username", 
              help="Username for the FTP connection. Alternatively set HOWIS_FTP_USERNAME.",
              default=lambda: os.environ.get("HOWIS_FTP_USERNAME", ""))
@click.option("-w",
              "password",
              help="Prompt for password (place the flag at the end of the command!). Alternatively set HOWIS_FTP_PASSWORD.",
              prompt=True, 
              prompt_required=False,
              hide_input=True)
@click.option("-e",
              "--encoding",
              help="Encoding to use for reading remote files.",
              default="ISO-8859-1")
@click.argument("ftp_url")
def main(username: str, password: str, ftp_url: str, encoding: str):
    
    password = password if password else os.environ.get("HOWIS_FTP_PASSWORD", "")
    if not password:
        logger.error("HOWIS_FTP_PASSWORD is not set! Use -w flag for password prompt.")
        exit(1)
    
    logger.info(f"Ingest latest data from '{ftp_url}'")
    
    try:
        ftp = FTP(ftp_url, encoding=encoding)
        ftp.login(user=username, passwd=password)
    
        utils.list_content(ftp)
        kontakt = parser.parse_kontakt(ftp)
        pegelstamm = parser.parse_pegelstamm(ftp)
        pegeldaten = parser.parse_pegeldaten(ftp)
        
        csa_staging.stage_systems(kontakt, pegelstamm)
        
    except Exception as e:
        logger.error("Failed to parse content!")
        raise e
    finally:
        if ftp:
            logger.debug("Closing FTP connection.")
            ftp.close()
    
    logger.info("done!")


if __name__ == '__main__':  # pragma: no cover
    main()  # pylint: disable=no-value-for-parameter
