import os
import uuid
import json
import csv

from functools import reduce
from loguru import logger
from typing import List

from collections.abc import Mapping

from howis_ingestor.parser import Kontakt, Pegelstamm, Pegeldaten


STAGING_SYSTEM = "%s_system.json"
STAGING_FEATURE = "%s_feature.json"
STAGING_DATASTREAM = "%s_datastream.json"
STAGING_OBSERVATION = "%s_observation.json"
STAGING_OBSERVATIONS = "%s_observations.csv"

CSV_DELIMITER = ","
UUID_NAMESPACE = uuid.UUID("{f964a9cf-b4d1-3de7-8f59-d6b885a7fb56}")


def _unique_id(discriminator: str):
    """Resolves reproducable unique ID based on the given input."""
    # create a non-random uuid from namepace and file
    return str(uuid.uuid3(namespace=UUID_NAMESPACE, name=discriminator))

class Resource:
    def __init__(self, id: uuid, file: str, parent_id: str):
        self.file = file
        self.parent_id = parent_id
        self.id = id

    @property
    def payload(self):
        with open(self.file) as payload:
            return json.load(payload)

class ObservationBuffer:
    
    def __init__(self, observations_csv: str):
        self.observations_csv = observations_csv
        self.temp_file = f"{observations_csv}.temp"
        self.buffer = []

    def __enter__(self):
        with open(self.observations_csv, "r", newline='', encoding="utf-8") as csv_file:
            reader = csv.reader(csv_file)
            self.buffer = list(reader)
        return self

    def __exit__(self, type, value, traceback):
        with open(self.temp_file, 'w', newline='') as csv_file:
            writer = csv.writer(csv_file)
            # do not count the header row
            logger.debug(f"Keep in buffer {len(self.buffer) - 1} not ingested rows.")
            writer.writerows(self.buffer)
        os.replace(self.temp_file, self.observations_csv)

    def __len__(self):
        return len(self.buffer)

    def __iter__(self):
        for _, row_values in enumerate(self.buffer):
            # skip the header row
            if row_values and not row_values[0].startswith("#"):
                zeit = row_values[0]
                wert = row_values[1]
                #einheit = row_values[2]
                datastream_id = row_values[3]
                id = _unique_id(f"{datastream_id}_{zeit}")
                
                yield {
                    "id": id,
                    "parent_id": datastream_id,
                    "payload": {
                        "id": id,
                        "datastream@id": datastream_id,
                        "resultTime": zeit,
                        "result": float(wert)
                    },
                    "row": row_values
                }

    def remove_from_buffer(self, row):
        try:
            self.buffer.remove(row)
        except:
            logger.debug(f"Row does not exist and cannot removed from buffer: {row}")

class Stager:
    def __init__(self, stage_dir: str, csa_base_url: str):
        if not os.path.exists(stage_dir):
            raise Exception(f"Stage directory does not exist")
        self.stage_dir = stage_dir
        self.csa_base_url = csa_base_url

    def _absolute_file(self, filename):
        return str(os.path.join(self.stage_dir, filename))

    def stage_systems(
        self, kontakt: Kontakt, pegelstamm: List[Pegelstamm] = []
    ) -> List[Resource]:
        staged_systems = []
        for pegel in pegelstamm:
            pgnr = getattr(pegel, "pgnr")
            filename = STAGING_SYSTEM % pgnr
            stage_file = self._absolute_file(filename)
            system_id = _unique_id(filename)
            stub = {
                "id": system_id,
                "type": "SimpleProcess",
                "definition": "http://www.w3.org/ns/sosa/Sensor",
                "uniqueId": getattr(pegel, "pegelseite-url"),
                "description": f"HOWIS Pegel for {pgnr}",
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
                    },
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
                        # },
                        # {
                        #     # provide role via parser
                        #     "role": "Technischer Kontakt",
                        #     "title": "Individual",
                        #     "individualName": getattr(kontakt, "name-techn"),
                        #     "address": {
                        #         "electronicMailAddress": getattr(kontakt, "email-techn"),
                        #     },
                        #     "phone": {
                        #         "voice": getattr(kontakt, "telefon-techn")
                        #     },
                    }
                ],
                "featuresOfInterest": [
                    {
                        "title": getattr(pegel, "gewaesser"),
                        "href": "https://en.wikipedia.org/wiki/Erft",
                        "type": "text/html",
                    }
                ],
            }

            with open(stage_file, "w") as system:
                system.write(json.dumps(stub, indent=2))

            staged_systems.append(Resource(system_id, stage_file, None))
            
        logger.info(f"Staged systems: {len(staged_systems)}")
        return staged_systems

    def stage_features(self, pegelstamm: List[Pegelstamm] = []) -> List[Resource]:
        staged_features = []
        for pegel in pegelstamm:
            pgnr = getattr(pegel, "pgnr")
            system_id = _unique_id(STAGING_SYSTEM % pgnr)
            staging_filename = STAGING_FEATURE % pgnr
            stage_file = self._absolute_file(staging_filename)
            feature_id = _unique_id(staging_filename)
            stub = {
                "id": feature_id,
                "type": "Feature",
                "properties": {
                    "uid": f"urn:x-erftverband:pegel:{pgnr}:sf",
                    "name": pgnr,
                    "label": getattr(pegel, "pgname"),
                    "land_id": getattr(pegel, "land-id"),
                    "group": getattr(pegel, "gruppe"),
                    "href": getattr(pegel, "pegelseite-url"),
                    "featureType": "http://www.opengis.net/def/samplingFeatureType/OGC-OM/2.0/SF_SamplingPoint",
                    "sampledFeature@link": {
                        "title": getattr(pegel, "gewaesser"),
                        "href": "https://en.wikipedia.org/wiki/Erft",
                        "type": "text/html",
                    },
                },
                "geometry": getattr(pegel, "geometry"),
            }

            with open(stage_file, "w") as system:
                system.write(json.dumps(stub, indent=2))

            staged_features.append(Resource(feature_id, stage_file, system_id))
        
        logger.info(f"Staged features: {len(staged_features)}")
        return staged_features

    def _resolve_first_observation(self, observations, default_value):
        if os.path.exists(observations):
            with open(observations) as obs_csv:
                reader = csv.reader(obs_csv)
                next(reader, None)  # skip the header
                row = next(reader, [])
                return row[0] if len(row) > 0 else default_value
        else:
            return default_value

    def stage_datastreams(
        self,
        pegelstamm: List[Pegelstamm] = [],
        pegeldaten: Mapping[str, Pegeldaten] = {},
    ) -> List[Resource]:
        def _assign(acc, value):
            acc[getattr(value, "pgnr")] = value
            return acc

        pgnr_to_pegelstamm = reduce(
            lambda acc, value: _assign(acc, value), pegelstamm, {}
        )

        staged_datastreams = []
        for pgnr, daten in pegeldaten.items():
            system_id = _unique_id(STAGING_SYSTEM % pgnr)
            stage_filename = STAGING_DATASTREAM % pgnr
            stage_file = self._absolute_file(stage_filename)
            datastream_id = _unique_id(stage_filename)

            pegel = pgnr_to_pegelstamm[pgnr]
            pegelname = getattr(pegel, "pgname")
            gewaesser = getattr(pegel, "gewaesser")
            zeit = getattr(daten, "zeit").isoformat()

            observations = self._absolute_file(STAGING_OBSERVATIONS % pgnr)
            first_observation = self._resolve_first_observation(observations, zeit)

            # http://media.hochwasserzentralen.de/lhp.dtd
            stub = {
                "id": datastream_id,
                "name": f"Water level for {pegelname} ({gewaesser})",
                "formats": ["application/json"],
                "system@link": {
                    "href": f"{self.csa_base_url}/systems/{system_id}",
                },
                "observedProperties": [
                    {
                        "label": "Water Level",
                        "description": "Erft Water Level",
                        # this actually describes the gauge sensor
                        "definition": "http://vocab.nerc.ac.uk/collection/L05/current/377/",
                    }
                ],
                "phenomenonTime": [first_observation, zeit],
                "resultTime": [first_observation, zeit],
                "type": "observation",
                "resultType": "measure",
                "schema": {
                    "obsFormat": "application/om+json",
                    "recordSchema": {
                        "type": "Quantity",
                        "definition": "http://purl.dataone.org/odo/ECSO_00001203",
                        "label": "Water Level",
                        "description": "The level of water.",
                        "uom": {"code": "cm"},
                        "nilValues": [
                            { "reason": "http://www.opengis.net/def/nil/OGC/0/missing", "value": "NaN" },
                            { "reason": "http://www.opengis.net/def/nil/OGC/0/BelowDetectionRange", "value": "-Infinity" },
                            { "reason": "http://www.opengis.net/def/nil/OGC/0/AboveDetectionRange", "value": "+Infinity" }
                        ]
                        
                        ## Alternative: DataRecord ~> Complex Observations
                        # "type": "DataRecord",
                        # "fields": [
                        #     {
                        #         "name": "time",
                        #         "type": "Time",
                        #         "definition": "http://www.opengis.net/def/property/OGC/0/SamplingTime",
                        #         "referenceFrame": "http://www.opengis.net/def/trs/BIPM/0/UTC",
                        #         "label": "Sampling Time",
                        #         "uom": {
                        #             "href": "http://www.opengis.net/def/uom/ISO-8601/0/Gregorian"
                        #         },
                        #     },
                        #     {
                        #         "name": "level",
                        #         "type": "Quantity",
                        #         "definition": "http://purl.dataone.org/odo/ECSO_00001203",
                        #         "label": "Water Level",
                        #         "description": "The level of water.",
                        #         "uom": {"code": "cm"},
                        #         # "nilValues": [
                        #         #     { "reason": "http://www.opengis.net/def/nil/OGC/0/missing", "value": "NaN" },
                        #         #     { "reason": "http://www.opengis.net/def/nil/OGC/0/BelowDetectionRange", "value": "-Infinity" },
                        #         #     { "reason": "http://www.opengis.net/def/nil/OGC/0/AboveDetectionRange", "value": "+Infinity" }
                        #         # ]
                        #     },
                        # ],
                    },
                    "encoding": {"type": "JSONEncoding"},
                },
                "live": False,
            }

            with open(stage_file, "w") as datastream:
                datastream.write(json.dumps(stub, indent=2))
            staged_datastreams.append(Resource(datastream_id, stage_file, system_id))

        logger.info(f"Staged datastreams: {len(staged_datastreams)}")
        return staged_datastreams

    def _append_to(self, csv_file, datastream_id, zeit, wert, einheit):
        is_new_file = not os.path.exists(csv_file)

        updated = False
        last_line = None
        if not is_new_file:
            last_line = self._last_line(csv_file)

        def write_header(csvfile):
            delimiter = CSV_DELIMITER
            # write header row as comment
            header = ["zeit", "wert", "einheit", "datastream"]
            csvfile.write("#" + delimiter.join(header)+ "\n")
        
        if is_new_file:
            with open(csv_file, "w+", newline='', encoding="utf-8") as csvfile:
                write_header(csvfile)    
        
        with open(csv_file, "r+", newline='', encoding="utf-8") as csvfile:
            first_line = csvfile.readline()
            if not first_line:
                write_header(csvfile)
            else:
                # Ensure header is commented out
                remaining_content = csvfile.read()
                if first_line and not first_line.startswith("#"):
                    csvfile.seek(0)
                    csvfile.write(f"#{first_line}{remaining_content}")
                    csvfile.truncate()
            
        if not last_line or last_line and not last_line.startswith(zeit):
            with open(csv_file, "a", newline='', encoding="utf-8") as csvfile:
                writer = csv.writer(csvfile, delimiter=CSV_DELIMITER, lineterminator="\n")
                writer.writerow([zeit, wert, einheit, datastream_id])
                updated = True
                
        return updated

    def _last_line(self, filepath: str) -> str:
        with open(filepath, "rb") as file:
            try:
                # Go to the end of the file before the last break-line
                file.seek(-2, os.SEEK_END)
                # Keep reading backward until you find the next break-line
                while file.read(1) != b"\n":
                    file.seek(-2, os.SEEK_CUR)
            except OSError:
                file.seek(0)
            return file.readline().decode()

    def stage_observations(
        self, pegeldaten: Mapping[str, Pegeldaten] = {}
    ) -> List[str]:
        staged_observations = []
        for pgnr, daten in pegeldaten.items():
            wert = getattr(daten, "wert")
            einheit = getattr(daten, "einheit")
            zeit = getattr(daten, "zeit").isoformat()

            datastream_id = _unique_id(STAGING_DATASTREAM % pgnr)
            observation_id = _unique_id(STAGING_OBSERVATION % zeit)
            csv_file = self._absolute_file(STAGING_OBSERVATIONS % pgnr)
            updated = self._append_to(csv_file, datastream_id, zeit, wert, einheit)
            if not updated:
                logger.debug(
                    f"Skip observation for datastream {datastream_id} with existing time at {zeit}"
                )
            else:
                stub = {
                    "id": observation_id,
                    "datastream@id": datastream_id,
                    "resultTime": zeit,
                    "result": wert
                }

                staged_file = self._absolute_file(STAGING_OBSERVATION % pgnr)
                with open(staged_file, "w") as observation:
                    observation.write(json.dumps(stub, indent=2))

            staged_observations.append(csv_file)
        return staged_observations
