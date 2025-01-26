import os
import tempfile
from pathlib import Path
from os.path import join
from ftplib import FTP

import click
from loguru import logger

from howis_ingestor import parser
from howis_ingestor.stager import Stager
from howis_ingestor.ingester import Ingestor


default_stage_dir = join(tempfile.gettempdir(), "howis_staging")


@click.command()
@click.option(
    "-u",
    "--ftp-username",
    default=lambda: os.environ.get("HOWIS_FTP_USERNAME", ""),
    help="Username for the FTP connection. Alternatively set HOWIS_FTP_USERNAME.",
)
@click.option(
    "-w",
    "--ftp-password",
    help="Prompt for ftp password (place the flag at the end of the command!). Alternatively set HOWIS_FTP_PASSWORD.",
    prompt=True,
    prompt_required=False,
    hide_input=True,
)
@click.option(
    "-s",
    "--stage-dir",
    default=default_stage_dir,
    help="Directory containing CSA data to be ingested.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Connect and parse HOWIS data but skips CSA ingestion.",
)
@click.option(
    "--override",
    is_flag=True,
    help="Allow to override existing CSA entities via PUT requests.",
)
@click.option(
    "-e",
    "--encoding",
    default="ISO-8859-1",
    help="Encoding to use for reading remote files.",
)
@click.option(
    "-d",
    "--destination",
    help="CSA URL for external access.",
)
@click.option(
    "--internal-destination",
    help="Internal URL for ingesting data. If empty --destination is tried.",
)
@click.argument("ftp_url")
def main(
    ftp_username: str,
    ftp_password: str,
    ftp_url: str,
    stage_dir: str,
    dry_run: bool,
    override: bool,
    encoding: str,
    destination: str,
    internal_destination: str,
):

    ftp_password = (
        ftp_password if ftp_password else os.environ.get("HOWIS_FTP_PASSWORD", "")
    )
    if not ftp_password:
        logger.error("HOWIS_FTP_PASSWORD is not set! Use -w flag for password prompt.")
        exit(1)

    csa_username = os.environ.get("HOWIS_CSA_USERNAME", None)
    if not csa_username:
        logger.info("HOWIS_CSA_USERNAME is not set!")
    csa_password = os.environ.get("HOWIS_CSA_PASSWORD", "")
    if not csa_password:
        logger.info("HOWIS_CSA_PASSWORD is not set!")

    if stage_dir == default_stage_dir:
        if not os.path.exists(default_stage_dir):
            logger.debug(f"Creating default stage_dir at '{default_stage_dir}'")
        Path(default_stage_dir).mkdir(exist_ok=True)
    logger.info(f"Using stage_dir at '{stage_dir}'.")

    logger.info(
        f"Establish connection with FTP username '{ftp_username}' to '{ftp_url}'"
    )
    with FTP(ftp_url, encoding=encoding) as ftp:
        try:
            ftp.login(user=ftp_username, passwd=ftp_password)
            #ftp.dir()  # print remote dir content

            kontakt = parser.parse_kontakt(ftp)
            pegelstamm = parser.parse_pegelstamm(ftp)
            pegeldaten = parser.parse_pegeldaten(ftp)
        except Exception as ftp_error:
            raise ftp_error

        try:
            external_csa_base_url = (
                destination.slice[-1] if destination.endswith("/") else destination
            ) if destination else "http://localhost:5000"  ## TODO csa impl currently expects system link
            
            stager = Stager(stage_dir=stage_dir, csa_base_url=external_csa_base_url)
            staged_systems = stager.stage_systems(kontakt, pegelstamm)
            staged_features = stager.stage_features(pegelstamm)
            staged_datastreams = stager.stage_datastreams(pegelstamm, pegeldaten)
            staged_observations = stager.stage_observations(pegeldaten)
            
            logger.info("#############################")
            logger.info("#")
            logger.info(f"Staged systems: {len(staged_systems)}")
            logger.info(f"Staged features: {len(staged_features)}")
            logger.info(f"Staged datastreams: {len(staged_datastreams)}")
            logger.info(f"Observation buffers: {len(staged_observations)}")
            logger.info("#")
            logger.info("#############################")
            
            ingestion_csa_base_url = internal_destination or external_csa_base_url
            logger.info(f"Ingest to endpoint: '{ingestion_csa_base_url}'")
            if ingestion_csa_base_url and not dry_run:
                ingestor = Ingestor(
                    stage_dir, ingestion_csa_base_url, csa_username, csa_password, override=override
                )
                ingestor.ingest_systems(staged_systems)
                ingestor.ingest_features(staged_features)
                ingestor.ingest_datastreams(staged_datastreams)
                ingestor.ingest_observations(staged_observations)
            else:
                if dry_run:
                    logger.warning("--dry-run is enabled.")
                elif not destination:
                    logger.warning("No ingestion destination URL provided!")
                logger.warning("Skipping ingestion.")

        except Exception as e:
            logger.error(f"Failed to ingest data: {e}")
            # if logger.level == "DEBUG":
            #     logger.exception(e)
            raise e

    logger.info("done!")


if __name__ == "__main__":  # pragma: no cover
    main()  # pylint: disable=no-value-for-parameter
