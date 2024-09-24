import os
import uuid
import json
import csv

from functools import reduce
from pathlib import Path
from os.path import join
from typing import List

import jsonpath
from loguru import logger
from collections.abc import Mapping

from howis_ingestor.parser import Kontakt, Pegelstamm, Pegeldaten


STAGING_SYSTEM = "%s_system.json"
STAGING_DATASTREAM = "%s_datastream.json"
STAGING_OBSERVATION = "%s_observation.json"
STAGING_OBSERVATIONS = "%s_observations.csv"


class Resource:
    def __init__(self, file: str, parent_id: str):
        self.file = file
        self.parent_id = parent_id

class Stager:
    
    def __init__(self, stage_dir: str, csa_base_url: str):
        if not os.path.exists(stage_dir):
            raise Exception(f"Stage directory does not exist")
        self.stage_dir = stage_dir
        self.csa_base_url = csa_base_url

    def _resolve(self, filename, pgnr):
        return str(os.path.join(self.stage_dir, filename % pgnr))

    def stage_systems(self, kontakt: Kontakt, pegelstamm: List[Pegelstamm] = []) -> List[Resource]:
        staged_systems = []
        for pegel in pegelstamm:
            system_id = str(uuid.uuid4())
            pgnr = getattr(pegel, "pgnr")
            stub = {
                "id": system_id,
                "type": "SimpleProcess",
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
                # "typeOf": {
                #     "href": "http://vocab.nerc.ac.uk/collection/L05/current/377/",
                # },
                "contacts": [
                    {
                        # provide role via parser
                        "role": "Bereitsteller",
                        "title": "Organization",
                        "organisationName": getattr(kontakt, "organisation-public"),
                        "address": {
                            "electronicMailAddress": getattr(kontakt, "email-public")
                        },
                    },
                    {
                        # provide role via parser
                        "role": "Technischer Kontakt",
                        "title": "Individual",
                        "individualName": getattr(kontakt, "name-techn"),
                        "address": {
                            "electronicMailAddress": getattr(kontakt, "email-techn"),
                        },
                        "phone": {
                            "voice": getattr(kontakt, "telefon-techn")
                        },
                        
                    }
                ],
                "featuresOfInterest": [
                    {
                        "title": getattr(pegel, "gewaesser"),
                        "href": "https://en.wikipedia.org/wiki/Erft",
                        "type": "gewaesser"
                    }
                ],
            }
            
            stage_file = self._resolve(STAGING_SYSTEM, pgnr)
            with open(stage_file, "w") as system:
                system.write(json.dumps(stub, indent=2))
                
            staged_systems.append(Resource(stage_file, None))
        return staged_systems


    def _resolve_id(self, json_file, pgnr: str):
        with open(self._resolve(json_file, pgnr)) as system:
            return jsonpath.findall("$.id", system)[0] or None


    def stage_datastreams(self, pegelstamm: List[Pegelstamm] = [], pegeldaten: Mapping[str, Pegeldaten] = {}) -> List[Resource]:
        def _assign(acc, value):
            acc[getattr(value, "pgnr")] = value
            return acc
        pgnr_to_pegelstamm = reduce(lambda acc, value: _assign(acc, value), pegelstamm, {})
        
        staged_datastreams = []
        for pgnr, daten in pegeldaten.items():
            system_id = self._resolve_id(STAGING_SYSTEM, pgnr)
            pegel = pgnr_to_pegelstamm[pgnr]
            datastream_id = str(uuid.uuid4())
            pegelname = getattr(pegel, "pgname")
            gewaesser = getattr(pegel, "gewaesser")
            stub = {
                "id": datastream_id,
                "name": f"Water level for {pegelname} ({gewaesser})",
                "formats": [ "application/json" ],
                "system@link": {
                    "href": f"{self.csa_base_url}/systems/{system_id}",
                },
                "observedProperties": [
                    {
                        "label": "Water Level",
                        "description": "Erft Water Level",
                        # this actually describes the gauge sensor
                        "definition": "http://vocab.nerc.ac.uk/collection/L05/current/377/"
                    }
                ],
                "phenomenonTime": [
                    getattr(daten, "zeit").isoformat(),
                    getattr(daten, "zeit").isoformat(),
                ],
                "resultTime":[
                    getattr(daten, "zeit").isoformat(),
                    getattr(daten, "zeit").isoformat(),
                ],
                "type": "observation",
                "resultType": "measure",
                "live": False,
            }
            
            staged_file = self._resolve(STAGING_DATASTREAM, pgnr)
            with open(staged_file, "w") as datastream:
                datastream.write(json.dumps(stub, indent=2))

            staged_datastreams.append(Resource(staged_file, system_id))
        
        return staged_datastreams
    
    def _append_to_csv(self, pgnr, datastream_id, zeit, wert, einheit):
        staged_file = self._resolve(STAGING_OBSERVATIONS, pgnr)
        is_new_file = not os.path.exists(staged_file)
        with open(staged_file, "a") as csvfile:
            writer = csv.writer(csvfile, lineterminator="\n")
            if is_new_file:
                writer.writerow(["zeit", "wert", "einheit", "datastream"])
            writer.writerow([zeit, wert, einheit, datastream_id])
        return csvfile
    

    def stage_observations(self, pegeldaten: Mapping[str, Pegeldaten] = {}) -> List[Resource]:
        staged_observations = []
        for pgnr, daten in pegeldaten.items():
            wert = getattr(daten, "wert")
            einheit = getattr(daten, "einheit")
            zeit = getattr(daten, "zeit").isoformat()
            
            datastream_id = self._resolve_id(STAGING_DATASTREAM, pgnr)
            csv_file = self._append_to_csv(pgnr, datastream_id, zeit, wert, einheit)
            stub = {
                "id": str(uuid.uuid4()),
                "datastream@id": datastream_id,
                "resultTime": zeit,
                "result": wert
                # TODO einheit?!
            }
            
            staged_file = self._resolve(STAGING_OBSERVATION, pgnr)
            with open(staged_file, "w") as observation:
                observation.write(json.dumps(stub, indent=2))
            
            staged_observations.append(Resource(staged_file, datastream_id))
            
        return staged_observations
