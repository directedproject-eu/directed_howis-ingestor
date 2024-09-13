import os
import json
import requests


from typing import List
from os.path import join
from loguru import logger

class Ingestor:
    
    def __init__(self, stage_dir: str, csa_base_url: str):
        if not os.path.exists(stage_dir):
            raise Exception(f"Stage directory does not exist")
        self.stage_dir = stage_dir
        self.csa_base_url = csa_base_url
    
    def _ingest_files(self, url: str, headers: dict = {}, files: List[str] = []):
        for file in files:
            with open(file) as payload:
                json_paylod = json.load(payload)
                response = requests.post(url, headers=headers, json=json_paylod)
                if response.status_code >= 400:
                    logger.warning(f"Failed to ingest {file}")
    
    def ingest_systems(self, systems):
        url = f"{self.csa_base_url}/systems"
        self._ingest_files(url, files=systems, headers={
            "content-type": "application/sml+json"
        })

    def ingest_datastreams(self, datastreams):
        url = f"{self.csa_base_url}/datastreams"
        self._ingest_files(url, files=datastreams, headers={
            "content-type": "application/json"
        })

    def ingest_observations(self, observations):
        url = f"{self.csa_base_url}/observations"
        self._ingest_files(url, files=observations, headers={
            "content-type": "application/om+json"
        })
