import os
import uuid
import json
from pathlib import Path
from os.path import join
from typing import List

from loguru import logger
from howis_ingestor.parser import Pegelstamm, Kontakt

def _resolve(local_path):
    return str(join(Path(__file__).parent, local_path))

STAGING_SYSTEM = _resolve("./staging/%s_system.json")
STAGING_DATASTREAM = _resolve("./staging/%s_datastream.json")

def stage_systems(kontakt: Kontakt, pegelstamm: List[Pegelstamm] = []):
    try:
        for pegel in pegelstamm:
            pgnr = getattr(pegel, "pgnr")
            if not os.path.exists(STAGING_SYSTEM % pgnr):
                stub = {
                    "id": str(uuid.uuid4()),
                    "type": "PhysicalSystem",
                    "definition": "http://www.w3.org/ns/sosa/Sensor",
                    "uniqueId": getattr(pegel, "pegelseite-url"),
                    "description": "HOWIS Pegel",
                    "label": getattr(pegel, "pgname"),
                    "identifiers": [
                        {
                            "label": "Pegelnummer",
                            "value": pgnr,
                        },
                        {
                            "label": "Pegelname",
                            "value": getattr(pegel, "pgname"),
                        },
                        {
                            "label": "Pegelgruppe",
                            "value": getattr(pegel, "gruppe"),
                        },
                        {
                            "label": "Pegelseite",
                            "value": getattr(pegel, "pegelseite-url"),
                        }
                    ],
                    "contacts": [
                        {
                            # provide role via parser
                            "role": "Bereitsteller",
                            "individualName": getattr(kontakt, "name-public"),
                            "organisationName": getattr(kontakt, "organisation-public"),
                        },
                        {
                            # provide role via parser
                            "role": "Technischer Kontakt",
                            "name": getattr(kontakt, "name-techn"),
                            "link": getattr(kontakt, "email-techn"),
                            "phone": getattr(kontakt, "telefon-techn"),
                            "address": {
                                "electronicMailAddress": getattr(kontakt, "email-techn")
                            }
                        }
                    ],
                    "featuresOfInterest": {
                        "title": getattr(pegel, "gewaesser"),
                        "href": "https://en.wikipedia.org/wiki/Erft",
                        "type": "gewaesser"
                    },
                }
                
                with open(STAGING_SYSTEM % pgnr, "w") as system:
                    system.write(json.dumps(stub, indent=2))
                    
    except Exception as e:
        logger.error("An staging error occured!", e)
        raise e