from loguru import logger
from ftplib import FTP
from xml.etree import ElementTree as ET


FILE_KONTAKTE = "ev_kontakte.xml"
FILE_PEGELDATEN = "ev_pegeldaten.xml"
FILE_PEGELSTAMM = "ev_pegelstamm.xml"


def list_content(ftp: FTP):
    logger.debug(ftp.retrlines('LIST'))
    