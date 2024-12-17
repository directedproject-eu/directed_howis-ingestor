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
        override: bool = False
    ):
        if not os.path.exists(stage_dir):
            raise Exception(f"Stage directory does not exist")
        self.stage_dir = stage_dir
        self.override = override
        
        self.csa_base_url = csa_base_url
        if csa_username:
            credentials_bytes = f"{csa_username}:{csa_password}".encode()
            self.credentials_b64 = b64encode(credentials_bytes).decode()
        else:
            self.credentials_b64 = None

    def _ingest_files(
        self,
        post_url: str,
        put_url: str = None,
        headers: dict = {},
        resources: List[Resource] = [],
    ):
        if self.credentials_b64:
            headers["Authorization"] = f"Basic {self.credentials_b64}"
        for resource in resources:
            with open(resource.file) as payload:
                json_paylod = json.load(payload)
                
                resource_exists = False
                if self.override and put_url:
                    get_headers = headers | { "Accept": headers["content-type"]}
                    del get_headers["content-type"]
                    response = requests.get(
                        put_url % resource.id, 
                        headers=get_headers
                    )
                    resource_exists = response.status_code != 404
                
                if resource_exists:
                    if put_url:
                        response = requests.put(
                            put_url % resource.id, headers=headers, json=json_paylod
                        )
                    else:
                        logger.info("Skip update as no PUT URL provided.")
                else:
                    endpoint_url = (
                        post_url % resource.parent_id if resource.parent_id else post_url
                    )
                    response = requests.post(
                        endpoint_url, headers=headers, json=json_paylod
                    )
                if response.status_code >= 400:
                    logger.warning("Failed to ingest:")
                    logger.warning(f"  headers: {headers}")
                    logger.warning(f"  payload: {json_paylod}")
                    logger.warning(f"  response: {response.content.decode()}")

    def ingest_systems(self, systems: List[Resource]):
        self._ingest_files(
            post_url=f"{self.csa_base_url}/systems",
            put_url=f"{self.csa_base_url}/systems/%s",
            resources=systems,
            headers={"content-type": "application/sml+json"},
        )

    def ingest_features(self, features: List[Resource]):
        self._ingest_files(
            post_url=f"{self.csa_base_url}/systems/%s/samplingFeatures",
            put_url=f"{self.csa_base_url}/samplingFeatures/%s",
            resources=features,
            headers={"content-type": "application/geo+json"},
        )

    def ingest_datastreams(self, datastreams: List[Resource]):
        self._ingest_files(
            post_url=f"{self.csa_base_url}/systems/%s/datastreams",
            put_url=f"{self.csa_base_url}/datastreams/%s",
            resources=datastreams,
            headers={"content-type": "application/json"},
        )

    def ingest_observations(self, observations: List[Resource]):
        self._ingest_files(
            post_url=f"{self.csa_base_url}/datastreams/%s/observations",
            # PUT observations is not supported yet
            #put_url=f"{self.csa_base_url}/observations/%s",
            resources=observations,
            headers={"content-type": "application/om+json"},
        )
