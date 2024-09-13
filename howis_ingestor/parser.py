from typing import List
from datetime import datetime
from collections.abc import  Mapping

from loguru import logger
from ftplib import FTP
from xml.etree import ElementTree as ET
from pyproj import Transformer
from pyproj.crs import CRS


FILE_KONTAKTE = "ev_kontakte.xml"
FILE_PEGELDATEN = "ev_pegeldaten.xml"
FILE_PEGELSTAMM = "ev_pegelstamm.xml"


class Kontakt:
    def __init__(self, **kwargs):
        for (key, value) in kwargs.items():
            setattr(self, key, value)

class Pegelstamm:
    def __init__(self, **kwargs):
        for (key, value) in kwargs.items():
            setattr(self, key, value)
            
class Pegeldaten:
    def __init__(self, **kwargs):
        for (key, value) in kwargs.items():
            setattr(self, key, value)
            

def parse_kontakt(ftp: FTP):
    parser = ET.XMLPullParser(['end'])
    ftp.retrlines(f"RETR {FILE_KONTAKTE}", parser.feed)
    
    kontakt = {}
    for event, elem in parser.read_events():
        if elem.tag not in ["lhp-daten", "kontakte", "infos"]:
            kontakt[elem.tag] = elem.text
        # if elem.tag == "infos":
        #     elem.iter("disclaimer")
    return Kontakt(**kontakt)

def parse_pegelstamm(ftp: FTP) -> List[Pegelstamm]:
    parser = ET.XMLPullParser(['start', 'end'])
    ftp.retrlines(f"RETR {FILE_PEGELSTAMM}", parser.feed)
    
    pegelstamm = []
    srs_code = "EPSG:4326"
    current_pegel = None
    for event, elem in parser.read_events():
        if event == "start" and elem.tag == "lhp-daten":
            srs_code = int(elem.get("surveyCRS", srs_code).split(":")[-1])

        if elem.tag == "pegelstamm":
            if event == "start":
                current_pegel = {}
            if event == "end":
                pegel = Pegelstamm(**current_pegel)
                pegelstamm.append(pegel)
                current_pegel = None
        elif current_pegel is not None:
            if elem.tag == "koordinaten":
                value = elem.text.split(",")
                koordinaten = list(map(lambda s: float(s.strip()), value))
                t = Transformer.from_crs(CRS(srs_code), CRS(4326), always_xy=True)
                current_pegel["geometry"] = {
                    "type": "Point",
                    "coordinates": t.transform(koordinaten[0], koordinaten[1])
                    
                }
            else:
                # parse pegelstamm element
                current_pegel[elem.tag] = elem.text

    
    logger.debug(f"Parsed {len(pegelstamm)} pegelstamm entities.")
    return pegelstamm


def parse_pegeldaten(ftp: FTP) -> Mapping[str, Pegeldaten]:
    parser = ET.XMLPullParser(['start', 'end'])
    ftp.retrbinary(f"RETR {FILE_PEGELDATEN}", parser.feed)
    
    pegeldaten = {}
    current_pegel = None
    time_range = (datetime.max, datetime.min)
    for event, elem in parser.read_events():
        if elem.tag == "pegeldaten":
            if event == "start":
                current_pegel = {}
            if event == "end":
                daten = Pegeldaten(**current_pegel)
                pgnr = getattr(daten, "pgnr")
                pegeldaten[pgnr] = daten
                current_pegel = None
        elif current_pegel is not None:
            # parse pegeldata element
            if elem.tag == "zeit":
                zeit = datetime.strptime(elem.text, "%d.%m.%Y %H:%M")
                current_pegel["zeit"] = zeit
                time_range = (min(time_range[0], zeit), max(time_range[1], zeit))
            elif elem.tag == "wert":
                wert = float(elem.text)
                current_pegel["wert"] = wert
            else:
                current_pegel[elem.tag] = elem.text
            
            if elem.get("einheit"):
                current_pegel["einheit"] = elem.get("einheit")
    
    logger.debug(f"Parsed {len(pegeldaten)} data entities ranging from {str(time_range[0])}--{str(time_range[1])}.")
    return pegeldaten
