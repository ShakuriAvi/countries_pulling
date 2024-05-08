import json
from enum import Enum

from requests import Response

from entity.source import Source
import requests
from typing import Dict, List, Any


class Countries(Source):
    api_url = "https://restcountries.com/v3.1/all"
    fields = ["name", "continents", "languages", "population"]

    def __init__(self):
        super().__init__()
        self.countries: List[Dict[str, str]] = []

    def pulling_data(self, *args, **kwargs):
        api_res: Response = requests.session().get(self.api_url)
        countries_api_res: List[Dict[str, Any]] = json.loads(api_res.text)
        countries_etl_lst: List[Dict[str, Any]] = []
        for country in countries_api_res:
            country_obj: Dict[str, Any] = dict()
            country_obj["name"] = country["name"]["common"]
            country_obj["continents"] = country["continents"][0]
            country_obj["languages"] = next(iter(country["languages"].values())) if "languages" in country \
                else "unknown"
            country_obj["population"] = str(country["population"])

            countries_etl_lst.append(country_obj)

        self.countries = countries_etl_lst

    def make_to_dynamodb(self) -> List[Dict[str, Dict]]:
        db_entity: List[Dict[str, Dict]] = []
        for item in self.countries:
            props: Dict[str, Dict] = {
                "country": {"S": item['name']},
                "languages": {"S": item['languages']},
                "region": {"S": item['continents']},
                "population": {"N": item['population']},
            }
            db_entity.append(props)
        return db_entity
