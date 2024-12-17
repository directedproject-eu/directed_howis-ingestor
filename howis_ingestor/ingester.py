import os
import json
import requests

from typing import List
from base64 import b64encode
from loguru import logger

from howis_ingestor.stager import Resource


class Ingestor:
    def __init__(
        self,
        stage_dir: str,
        csa_base_url: str,
        csa_username: str = None,
        csa_password: str = "",
    ):
        if not os.path.exists(stage_dir):
            raise Exception(f"Stage directory does not exist")
        self.stage_dir = stage_dir
        self.csa_base_url = csa_base_url
        if csa_username:
            credentials_bytes = f"{csa_username}:{csa_password}".encode()
            self.credentials_b64 = b64encode(credentials_bytes).decode()
        else:
            self.credentials_b64 = None

    def _ingest_files(
        self, url: str, headers: dict = {}, resources: List[Resource] = []
    ):
        if self.credentials_b64:
            headers["Authorization"] = f"Basic {self.credentials_b64}"
        for resource in resources:
            with open(resource.file) as payload:
                json_paylod = json.load(payload)
                endpoint_url = url % resource.parent_id if resource.parent_id else url
                response = requests.post(
                    endpoint_url, headers=headers, json=json_paylod
                )
                if response.status_code >= 400:
                    logger.warning(
                        f"Failed to ingest {payload}: {response.content.decode()}"
                    )

    def ingest_systems(self, systems: List[Resource]):
        url = f"{self.csa_base_url}/systems"
        self._ingest_files(
            url, resources=systems, headers={"content-type": "application/sml+json"}
        )

    def ingest_features(self, features: List[Resource]):
        url = f"{self.csa_base_url}/systems/%s/samplingFeatures"
        self._ingest_files(
            url, resources=features, headers={"content-type": "application/geo+json"}
        )

    def ingest_datastreams(self, datastreams: List[Resource]):
        url = f"{self.csa_base_url}/systems/%s/datastreams"
        self._ingest_files(
            url, resources=datastreams, headers={"content-type": "application/json"}
        )

    def ingest_observations(self, observations: List[Resource]):
        url = f"{self.csa_base_url}/datastreams/%s/observations"
        self._ingest_files(
            url, resources=observations, headers={"content-type": "application/om+json"}
        )
